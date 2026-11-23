"""质量诊断：分析日志解析质量，生成完整性、成功率、延迟分布等诊断报告。

在线采集质量诊断（``diagnose_source``）与回溯质量诊断（``diagnose_backfill_quality``）
使用各自独立的输入：前者只看在线采集/解析文件，后者只看某个回溯任务实际落盘的
明细行，两者互不共享，也不会把同源的另一个回溯任务混进来。
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService, parse_time_bounds
from app.store import store

# 回溯明细 raw 行的规范格式，用于判定“成功行 / 异常行”。
BACKFILL_LINE_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) \[backfill\] (?P<level>[A-Z]+) "
    r"source=(?P<source>\S+) task=(?P<task>\S+) record_id=(?P<record_id>\d+)"
    r"(?: batch=(?P<batch>\d+))?(?: (?P<message>.*))?$"
)
_MISSING = object()


class LogQualityService(BaseModuleService):
    MODULE = "log_quality"
    ENTITY = "诊断报告"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["报告编号", "关联源", "解析成功率", "字段完整性", "延迟分布", "脏数据类型", "诊断时间", "报告状态"]
    REQUIRED_FIELDS = ["报告编号", "关联源", "解析成功率"]
    STATUS_ORDER = ["诊断中", "已生成", "已复核", "已归档"]
    ACTION_RULES = {"确认生成": "已生成", "标记复核": "已复核", "提交归档": "已归档"}
    NEGATIVE_ACTIONS = []

    def diagnose_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源进行质量诊断。"""
        parsed_file = self._data_dir("logs") / f"{source_key}_parsed.jsonl"
        parse_error_file = self._data_dir("logs") / f"{source_key}_parse_errors.jsonl"
        log_file = self._data_dir("logs") / f"{source_key}.jsonl"

        if not parsed_file.exists() and not log_file.exists():
            return {"error": "无可用数据", "diagnosed": False, "source_key": source_key}

        parsed_records = self._read_jsonl(parsed_file) if parsed_file.exists() else []
        error_records = self._read_jsonl(parse_error_file) if parse_error_file.exists() else []
        all_records = self._read_jsonl(log_file) if log_file.exists() else parsed_records

        total = len(all_records) + len(error_records)
        parsed_ok = len(parsed_records)
        parse_errors = len(error_records)

        success_rate = round(parsed_ok / max(total, 1) * 100, 2) if total > 0 else 100

        completeness = self._compute_completeness(parsed_records)
        latency_dist = self._compute_latency_distribution(parsed_records)
        dirty_types = self._classify_dirty_data(error_records)

        report = {
            "report_id": f"QL_{source_key}_{int(time.time())}",
            "source_key": source_key,
            "diagnosed_at": time.time(),
            "parse_success_rate": success_rate,
            "parsed_ok": parsed_ok,
            "parse_errors": parse_errors,
            "field_completeness": completeness,
            "latency_distribution": latency_dist,
            "dirty_data_types": dirty_types,
            "total_records": total,
        }

        self._write_jsonl(
            self._data_dir("exports") / "quality_reports.jsonl",
            [report]
        )

        self._write_checkpoint(f"quality_{source_key}", report)

        # 诊断完成后自动触发告警评估
        if success_rate < 95 or parse_errors > 0:
            alert_r = self._trigger_alert_evaluation(source_key, {
                "parse_success_rate": success_rate,
                "field_completeness": completeness.get("overall", 100),
                "parse_errors": parse_errors,
                "source_key": source_key,
            })
            if "error" not in alert_r:
                report["auto_alert"] = alert_r

        return report

    # ------------------------------------------------------------------
    # 独立回溯质量报告
    # ------------------------------------------------------------------

    def _backfill_state(self, task_id: str, source_key: str | None = None) -> dict[str, Any]:
        """回溯任务元信息：优先 checkpoint，其次回溯任务登记行。"""
        cp = self._read_checkpoint(f"backfill_{task_id}") or {}
        task = None
        for row in store.rows("backfill_range"):
            if str(row.get("任务编号")) == task_id:
                task = row
                break
        task = task or {}
        return {
            "task_id": task_id,
            "source_key": source_key or cp.get("source_key") or task.get("关联源", ""),
            "time_range": cp.get("time_range") or task.get("时间范围", ""),
            "cp": cp,
            "task": task,
            "detail_file": cp.get("detail_file") or "",
        }

    def _task_detail_name(self, task_id: str, source_key: str) -> str:
        return f"{source_key}_backfill_{task_id}.jsonl"

    def _resolve_detail_file(
        self, state: dict[str, Any], task_id: str
    ) -> Path:
        source_key = state["source_key"]
        explicit = state.get("detail_file")
        if explicit:
            path = Path(str(explicit))
            if path.exists():
                return path
        task_file = self._data_dir("logs") / self._task_detail_name(task_id, source_key)
        if task_file.exists():
            return task_file
        # 兼容历史数据：旧版本把同源所有回溯任务追加到同一个镜像文件。
        # 只有任务确实采集过（checkpoint 有行数）才回退，避免空任务误读别人的数据。
        collected = int(state["cp"].get("line_no") or state["cp"].get("record_count") or 0)
        legacy = self._data_dir("logs") / f"{source_key}_backfill.jsonl"
        if collected > 0 and legacy.exists():
            return legacy
        return task_file

    def _iter_detail_lines(self, path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """逐行读取明细：返回 (可解析记录, 无法解析的坏 JSON 行证据)。"""
        records: list[dict[str, Any]] = []
        broken: list[dict[str, Any]] = []
        if not path.exists():
            return records, broken
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                text = line.rstrip("\n")
                if not text.strip():
                    continue
                try:
                    record = json.loads(text)
                except json.JSONDecodeError as exc:
                    broken.append({
                        "file": str(path),
                        "file_line": lineno,
                        "raw_detail": text[:500],
                        "reason": f"明细行不是合法 JSON：{exc.msg}",
                    })
                    continue
                if not isinstance(record, dict):
                    broken.append({
                        "file": str(path),
                        "file_line": lineno,
                        "raw_detail": text[:500],
                        "reason": "明细行不是 JSON 对象",
                    })
                    continue
                record["_file_line"] = lineno
                records.append(record)
        return records, broken

    def _record_identity(self, record: dict[str, Any]) -> tuple[str, str]:
        """记录归属：优先明细字段，缺失时从原始行里解析，避免漏判混入数据。"""
        rec_task = str(record.get("task_id") or "")
        rec_source = str(record.get("source_key") or "")
        raw = record.get("raw")
        if isinstance(raw, str):
            match = BACKFILL_LINE_RE.match(raw)
            if match:
                rec_task = rec_task or match.group("task")
                rec_source = rec_source or match.group("source")
        return rec_task, rec_source

    def _bind_task_records(
        self,
        records: list[dict[str, Any]],
        task_id: str,
        source_key: str,
        filter_by_range: bool = True,
        bounds: tuple[datetime, datetime] | None = None,
        strict_binding: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """按 source + task 绑定明细；返回 (本任务记录, 被排除记录, 排除原因)。

        ``strict_binding`` 为真时表示读的是任务专属文件，此时归属字段缺失的记录
        仍会进入校验（从而作为坏行留证）；为假时表示读的是旧版共享镜像文件，
        无法证明归属的记录一律排除，避免线上记录或别的任务混入。
        """
        bound: list[dict[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        reasons: list[dict[str, Any]] = []
        for record in records:
            rec_task, rec_source = self._record_identity(record)
            reason = ""
            if rec_task and rec_task != task_id:
                reason = "other_task"
            elif rec_source and source_key and rec_source != source_key:
                reason = "other_source"
            elif filter_by_range and bounds is not None and self._record_outside_range(record, bounds):
                reason = "out_of_range"
            elif not strict_binding and not rec_task and not rec_source:
                reason = "unbound_record"
            if reason:
                excluded.append(record)
                reasons.append({
                    "task_id": rec_task,
                    "source_key": rec_source,
                    "reason": reason,
                    "file_line": record.get("_file_line"),
                })
                continue
            bound.append(record)
        return bound, excluded, reasons

    def _record_outside_range(self, record: dict[str, Any], bounds: tuple[datetime, datetime]) -> bool:
        ts_text = str(record.get("ts") or "")
        if not ts_text:
            raw = str(record.get("raw") or "")
            match = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", raw)
            ts_text = match.group(1) if match else ""
        if not ts_text:
            return False
        try:
            ts = datetime.strptime(ts_text, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return False
        start, end = bounds
        return not (start <= ts <= end)

    def _validate_backfill_line(
        self, record: dict[str, Any], task_id: str, source_key: str
    ) -> dict[str, Any] | None:
        """校验单条明细里的实际原始行；合法返回 None，异常返回坏行证据。"""
        raw = record.get("raw")
        if raw is None:
            raw = record.get("raw_line", _MISSING)
        if raw is _MISSING or raw == "":
            return self._bad_line(record, "missing_raw", "明细缺少原始行 raw")
        if not isinstance(raw, str):
            return self._bad_line(record, "raw_not_string", "原始行不是字符串")
        match = BACKFILL_LINE_RE.match(raw)
        if match is None:
            return self._bad_line(record, "format_mismatch", "原始行不符合回溯日志格式")
        if match.group("source") != source_key:
            return self._bad_line(record, "source_mismatch", "原始行 source 与任务关联源不一致")
        if match.group("task") != task_id:
            return self._bad_line(record, "task_mismatch", "原始行 task 与当前回溯任务不一致")
        record_id = record.get("record_id")
        if record_id is not None and str(record_id) != match.group("record_id"):
            return self._bad_line(record, "record_id_mismatch", "原始行 record_id 与明细字段不一致")
        ts_field = str(record.get("ts") or "")
        if ts_field and ts_field != match.group("timestamp"):
            return self._bad_line(record, "timestamp_mismatch", "原始行时间戳与明细字段不一致")
        return None

    def _bad_line(self, record: dict[str, Any], category: str, message: str) -> dict[str, Any]:
        return {
            "category": category,
            "reason": message,
            "task_id": record.get("task_id"),
            "source_key": record.get("source_key"),
            "record_id": record.get("record_id") or record.get("line_no"),
            "line_no": record.get("line_no"),
            "file_line": record.get("_file_line"),
            "raw": record.get("raw") if isinstance(record.get("raw"), str) else str(record.get("raw"))[:500],
        }

    def _range_proof(
        self,
        path: Path,
        records: list[dict[str, Any]],
        task_id: str,
        source_key: str,
        time_range: str,
    ) -> dict[str, Any]:
        record_ids = []
        line_nos = []
        timestamps = []
        for record in records:
            rid = record.get("record_id", record.get("line_no"))
            if isinstance(rid, (int, float)) or str(rid).isdigit():
                record_ids.append(int(rid))
            ln = record.get("line_no")
            if isinstance(ln, (int, float)) or str(ln).isdigit():
                line_nos.append(int(ln))
            ts = str(record.get("ts") or "")
            if not ts:
                match = re.match(r"^(\S+)", str(record.get("raw") or ""))
                ts = match.group(1) if match else ""
            if ts:
                timestamps.append(ts)
        bounds = parse_time_bounds(time_range)
        proof = {
            "task_id": task_id,
            "source_key": source_key,
            "time_range": time_range,
            "range_start": bounds[0].strftime("%Y-%m-%dT%H:%M:%S") if bounds else None,
            "range_end": bounds[1].strftime("%Y-%m-%dT%H:%M:%S") if bounds else None,
            "detail_file": str(path),
            "detail_sha256": self._file_sha256(path) if path.exists() else None,
            "record_count": len(records),
            "record_id_min": min(record_ids) if record_ids else None,
            "record_id_max": max(record_ids) if record_ids else None,
            "line_no_min": min(line_nos) if line_nos else None,
            "line_no_max": max(line_nos) if line_nos else None,
            "timestamp_min": min(timestamps) if timestamps else None,
            "timestamp_max": max(timestamps) if timestamps else None,
            "first_record_id": min(record_ids) if record_ids else None,
            "last_record_id": max(record_ids) if record_ids else None,
            "line_count": len(records),
            "proof": (
                f"task={task_id} source={source_key} range={time_range} "
                f"records={len(records)} record_id=[{min(record_ids) if record_ids else '-'},"
                f"{max(record_ids) if record_ids else '-'}] "
                f"ts=[{min(timestamps) if timestamps else '-'},{max(timestamps) if timestamps else '-'}]"
            ),
            "scoped_by": ["source_key", "task_id"],
        }
        if bounds and timestamps:
            parsed = []
            for ts in timestamps:
                try:
                    parsed.append(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S"))
                except ValueError:
                    continue
            proof["timestamps_within_range"] = all(bounds[0] <= ts <= bounds[1] for ts in parsed) if parsed else None
        else:
            proof["timestamps_within_range"] = None
        return proof

    def _file_sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _safe_sha256(self, path: Path) -> str | None:
        try:
            return self._file_sha256(path) if path.exists() else None
        except OSError:
            return None

    def _batch_key(self, task_id: str, source_key: str, time_range: str, detail_sha: str | None) -> str:
        """同一批确认报告的去重键：任务 + 源 + 范围 + 输入文件内容摘要。"""
        material = f"{task_id}|{source_key}|{time_range}|{detail_sha or 'no-input'}"
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

    def _report_paths(self, task_id: str) -> tuple[Path, Path, Path]:
        exports = self._data_dir("exports")
        safe_task = re.sub(r"[^A-Za-z0-9._-]+", "_", task_id)
        return (
            exports / f"backfill_quality_{safe_task}.json",
            exports / "backfill_quality_reports.jsonl",
            exports / f"backfill_quality_anomalies_{safe_task}.jsonl",
        )

    def _existing_report_for_key(self, batch_key: str) -> dict[str, Any] | None:
        """同批（同任务同输入）已确认报告：仅当报告文件完整可读时才复用。

        汇总索引在、但任务报告文件缺失或被中断写坏时返回 None，让重试重新
        生成完整报告；汇总索引本身按 batch_key 去重，不会因此产生重复条目。
        """
        summary_rows = [
            row for row in self._read_jsonl(self._data_dir("exports") / "backfill_quality_reports.jsonl")
            if row.get("batch_key") == batch_key and row.get("report_status") == "已确认"
        ]
        if not summary_rows:
            return None
        summary = summary_rows[-1]
        report_file, _, _ = self._report_paths(str(summary.get("task_id", "")))
        if not report_file.exists():
            return None
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
        if report.get("batch_key") != batch_key:
            return None
        return report

    def _merge_evidence(self, evidence_path: Path, anomalies: list[dict[str, Any]]) -> None:
        """异常证据按批追加并去重，历史坏行证据不会被后续报告覆盖。"""
        existing = self._read_jsonl(evidence_path)
        seen = {
            (str(row.get("batch_key")), row.get("file_line"), row.get("category"),
             str(row.get("record_id")), str(row.get("raw"))[:200])
            for row in existing
        }
        merged = list(existing)
        for anomaly in anomalies:
            key = (str(anomaly.get("batch_key")), anomaly.get("file_line"), anomaly.get("category"),
                   str(anomaly.get("record_id")), str(anomaly.get("raw"))[:200])
            if key not in seen:
                seen.add(key)
                merged.append(anomaly)
        self._atomic_write_jsonl(evidence_path, merged)

    def get_backfill_quality_report(
        self, task_id: str, source_key: str | None = None
    ) -> dict[str, Any] | None:
        """按任务读取回溯质量报告详情。

        最近一次生成失败且尚未重试成功时返回 None，避免把上一批确认报告当成
        本批结果；失败原因可通过 ``get_backfill_report_failure`` 查询。
        """
        state = self._backfill_state(task_id, source_key)
        cp = state["cp"]
        report_file, _, _ = self._report_paths(task_id)
        quality_cp = self._read_checkpoint(f"backfill_quality_{task_id}") or {}
        # 最近一次生成失败且还没重试成功时，不把上一批报告当作本批结果。
        if (quality_cp.get("report_status") == "失败"
                or cp.get("quality_report_status") == "失败"):
            return None
        if report_file.exists():
            try:
                report = json.loads(report_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return None
            # 只有报告对应的输入仍是当前明细时才返回，避免旧批次报告冒充新结果。
            current_sha = self._safe_sha256(self._resolve_detail_file(state, task_id))
            if current_sha and report.get("input_sha256") not in (None, current_sha):
                return None
            return report
        stored_sha = cp.get("quality_input_sha256")
        stored_task = cp.get("quality_task_id")
        for report in self._read_jsonl(self._data_dir("exports") / "backfill_quality_reports.jsonl"):
            if report.get("task_id") == task_id and report.get("report_status") == "已确认":
                if stored_task and stored_task != task_id:
                    continue
                if stored_sha and report.get("input_sha256") and report.get("input_sha256") != stored_sha:
                    continue
                return report
        return None

    def get_backfill_anomalies(
        self, task_id: str | None = None, source_key: str | None = None
    ) -> list[dict[str, Any]]:
        """历史异常证据回查：任务级优先，未指定任务时按源汇总。"""
        exports = self._data_dir("exports")
        if task_id:
            _, _, anomaly_file = self._report_paths(task_id)
            anomalies = self._read_jsonl(anomaly_file)
        else:
            anomalies = []
            for report in self._read_jsonl(exports / "backfill_quality_reports.jsonl"):
                if source_key and report.get("source_key") != source_key:
                    continue
                report_task = report.get("task_id")
                if not report_task:
                    continue
                _, _, anomaly_file = self._report_paths(str(report_task))
                for anomaly in self._read_jsonl(anomaly_file):
                    anomaly.setdefault("task_id", report_task)
                    anomalies.append(anomaly)
        if source_key:
            anomalies = [a for a in anomalies if a.get("source_key") == source_key]
        return anomalies

    def _record_failure(
        self, task_id: str, source_key: str, time_range: str, reason: str
    ) -> None:
        """把失败原因落到 checkpoint，供报告详情与重试判断使用。

        记录失败状态本身也是写文件；这里不允许它反过来打断原始错误的上报。
        """
        try:
            self._write_checkpoint(f"backfill_quality_{task_id}", {
                "task_id": task_id,
                "source_key": source_key,
                "time_range": time_range,
                "report_status": "失败",
                "failure_reason": reason,
                "failed_at": time.time(),
            })
        except Exception:
            pass

    def diagnose_backfill_quality(
        self,
        task_id: str,
        source_key: str | None = None,
        time_range: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """生成独立回溯质量报告：只依据该任务实际保存的回溯明细。

        - 输入严格绑定 source + task，排除在线采集文件、同源其它任务、范围外记录；
        - 从明细 raw 行校验成功行 / 异常行，并保留坏行证据；
        - 报告与异常证据均为原子写入；同一批（task+source+范围+输入摘要）重试
          时返回已有确认报告，不会重复生成。
        """
        state = self._backfill_state(task_id, source_key)
        source_key = state["source_key"]
        time_range = time_range if time_range is not None else state["time_range"]
        if not source_key:
            reason = f"回溯任务 {task_id} 缺少关联源，无法诊断"
            self._record_failure(task_id, source_key, time_range, reason)
            return {
                "error": reason,
                "task_id": task_id,
                "report_status": "失败",
                "diagnosed": False,
            }

        detail_file = self._resolve_detail_file(state, task_id)
        report_file, summary_file, anomaly_file = self._report_paths(task_id)

        if not detail_file.exists():
            reason = f"回溯任务 {task_id} 无实际采集明细：{detail_file}"
            self._record_failure(task_id, source_key, time_range, reason)
            return {
                "error": reason,
                "task_id": task_id,
                "source_key": source_key,
                "time_range": time_range,
                "report_status": "失败",
                "diagnosed": False,
                "detail_file": str(detail_file),
            }

        raw_records, broken_json = self._iter_detail_lines(detail_file)
        if not raw_records and not broken_json:
            reason = f"回溯任务 {task_id} 的明细文件为空：{detail_file}"
            self._record_failure(task_id, source_key, time_range, reason)
            return {
                "error": reason,
                "task_id": task_id,
                "source_key": source_key,
                "time_range": time_range,
                "report_status": "失败",
                "diagnosed": False,
                "detail_file": str(detail_file),
            }
        try:
            detail_sha = self._file_sha256(detail_file)
        except OSError as exc:
            reason = f"读取回溯明细失败：{exc}"
            self._record_failure(task_id, source_key, time_range, reason)
            return {
                "error": reason,
                "task_id": task_id,
                "source_key": source_key,
                "report_status": "失败",
                "diagnosed": False,
            }

        batch_key = self._batch_key(task_id, source_key, time_range, detail_sha)
        existing = self._existing_report_for_key(batch_key)
        if existing is not None and not force:
            cached = dict(existing)
            cached["report_status"] = "已确认"
            cached["reused"] = True
            self._register_report_row(cached)
            return cached

        bounds = parse_time_bounds(time_range)
        strict_binding = detail_file.name == self._task_detail_name(task_id, source_key)
        bound, excluded, exclude_reasons = self._bind_task_records(
            raw_records, task_id, source_key,
            # 任务专属文件里的越界行保留下来作为异常证据；旧版共享镜像里的越界行
            # 无法证明归属，直接排除，避免把另一个时间段的记录算进来。
            filter_by_range=bounds is not None and not strict_binding, bounds=bounds,
            strict_binding=strict_binding)

        anomalies: list[dict[str, Any]] = []
        valid_rows: list[dict[str, Any]] = []
        seen_record_ids: dict[str, int] = {}
        for record in bound:
            rec_key = str(record.get("record_id") or record.get("line_no") or "")
            bad = self._validate_backfill_line(record, task_id, source_key)
            if bad is None and rec_key and rec_key in seen_record_ids:
                bad = self._bad_line(
                    record, "duplicate_record",
                    f"record_id {rec_key} 在任务明细中重复出现")
                bad["duplicate_of_file_line"] = seen_record_ids[rec_key]
            if bad is None and bounds is not None and self._record_outside_range(record, bounds):
                bad = self._bad_line(
                    record, "out_of_range",
                    f"记录时间不在任务范围 {time_range} 内")
            if rec_key and rec_key not in seen_record_ids:
                seen_record_ids[rec_key] = record.get("_file_line")
            if bad is None:
                valid_rows.append(record)
            else:
                bad["source_key"] = source_key
                bad["task_id"] = task_id
                anomalies.append(bad)
        broken_evidence = []
        for item in broken_json:
            item["source_key"] = source_key
            item["task_id"] = task_id
            broken_evidence.append(item)
            anomalies.append({
                "category": "broken_json",
                "reason": item["reason"],
                "task_id": task_id,
                "source_key": source_key,
                "file_line": item.get("file_line"),
                "raw": item.get("raw_detail", ""),
            })

        total_rows = len(valid_rows) + len(anomalies)
        success_rate = round(len(valid_rows) / max(total_rows, 1) * 100, 2)
        categories = Counter(item["category"] for item in anomalies)
        categories["excluded_other_task"] = sum(
            1 for r in exclude_reasons if r.get("reason") == "other_task")
        categories["excluded_online_or_other"] = sum(
            1 for r in exclude_reasons if r.get("reason") in ("other_source", "out_of_range"))

        evidence_path = anomaly_file
        for anomaly in anomalies:
            anomaly["batch_key"] = batch_key
            anomaly["time_range"] = time_range
        report = {
            "report_id": f"QLB_{task_id}",
            "report_type": "backfill",
            "report_scope": "backfill_task",
            "source_key": source_key,
            "task_id": task_id,
            "backfill_task_id": task_id,
            "time_range": time_range,
            "report_status": "已确认",
            "confirmed": True,
            "diagnosed_at": time.time(),
            "batch_key": batch_key,
            "input_sha256": detail_sha,
            "input_file": str(detail_file),
            "diagnosis_input": {
                "source_key": source_key,
                "task_id": task_id,
                "time_range": time_range,
                "detail_file": str(detail_file),
                "detail_sha256": detail_sha,
                "line_count": len(raw_records),
                "bound_line_count": len(bound),
                "excluded_line_count": len(excluded),
                "exclude_reasons": exclude_reasons[:50],
                "online_records_used": 0,
                "other_tasks_used": 0,
            },
            "total_records": total_rows,
            "total_raw_lines": total_rows,
            "raw_line_count": total_rows,
            "total_lines": total_rows,
            "valid_raw_lines": len(valid_rows),
            "valid_records": len(valid_rows),
            "valid_lines": len(valid_rows),
            "valid_line_count": len(valid_rows),
            "parsed_ok": len(valid_rows),
            "bad_lines": len(anomalies),
            "bad_records": len(anomalies),
            "bad_line_count": len(anomalies),
            "parse_errors": len(anomalies),
            "parse_success_rate": success_rate,
            "valid_raw_line_count": len(valid_rows),
            "bad_raw_line_count": len(anomalies),
            "异常行数": len(anomalies),
            "有效原始行数": len(valid_rows),
            "失败原因": None,
            "abnormal_categories": dict(categories),
            "dirty_data_types": {"total": len(anomalies), "categories": dict(categories)},
            "range_proof": self._range_proof(detail_file, valid_rows, task_id, source_key, time_range),
            "evidence_file": str(evidence_path),
            "anomaly_evidence_count": len(anomalies),
            "notes": "回溯质量报告仅使用本任务回溯明细；在线采集与同源其它任务不计入。",
        }

        try:
            self._merge_evidence(evidence_path, anomalies)
            self._atomic_write_json(report_file, report)
        except Exception as exc:
            reason = f"质量报告写入失败：{exc}"
            self._record_failure(task_id, source_key, time_range, reason)
            return {
                "error": reason,
                "task_id": task_id,
                "source_key": source_key,
                "time_range": time_range,
                "report_status": "失败",
                "diagnosed": False,
                "valid_raw_lines": len(valid_rows),
                "bad_lines": len(anomalies),
                "batch_key": batch_key,
            }

        try:
            self._append_summary_once(summary_file, report)
        except Exception as exc:
            reason = f"质量报告汇总写入失败：{exc}"
            self._record_failure(task_id, source_key, time_range, reason)
            return {
                "error": reason,
                "task_id": task_id,
                "source_key": source_key,
                "time_range": time_range,
                "report_status": "失败",
                "diagnosed": False,
                "valid_raw_lines": len(valid_rows),
                "bad_lines": len(anomalies),
                "batch_key": batch_key,
                "report_file": str(report_file),
            }

        self._register_report_row(report)

        self._write_checkpoint(f"backfill_quality_{task_id}", {
            "task_id": task_id,
            "source_key": source_key,
            "time_range": time_range,
            "report_id": report["report_id"],
            "batch_key": batch_key,
            "input_sha256": detail_sha,
            "report_file": str(report_file),
            "evidence_file": str(evidence_path),
            "report_status": "已确认",
            "valid_raw_lines": len(valid_rows),
            "bad_lines": len(anomalies),
            "diagnosed_at": report["diagnosed_at"],
        })

        if len(anomalies) > 0 or success_rate < 95:
            alert_r = self._trigger_alert_evaluation(source_key, {
                "parse_success_rate": success_rate,
                "field_completeness": 100,
                "parse_errors": len(anomalies),
                "source_key": source_key,
                "task_id": task_id,
                "report_type": "backfill",
            })
            if "error" not in alert_r:
                report["auto_alert"] = alert_r

        return report

    def _append_summary_once(self, summary_file: Path, report: dict[str, Any]) -> None:
        """汇总索引按 batch_key 去重追加；中断后重试不会生成重复确认报告。"""
        existing = self._read_jsonl(summary_file)
        if any(row.get("batch_key") == report.get("batch_key")
               and row.get("report_status") == "已确认" for row in existing):
            return
        existing.append({
            "report_id": report.get("report_id"),
            "report_type": "backfill",
            "source_key": report.get("source_key"),
            "task_id": report.get("task_id"),
            "time_range": report.get("time_range"),
            "report_status": report.get("report_status"),
            "batch_key": report.get("batch_key"),
            "input_sha256": report.get("input_sha256"),
            "valid_raw_lines": report.get("valid_raw_lines"),
            "bad_lines": report.get("bad_lines"),
            "parse_success_rate": report.get("parse_success_rate"),
            "diagnosed_at": report.get("diagnosed_at"),
        })
        self._atomic_write_jsonl(summary_file, existing)

    def _register_report_row(self, report: dict[str, Any]) -> None:
        """把回溯报告登记进质量诊断列表，详情字段落在行内，按 report_id 幂等。"""
        report_id = str(report.get("report_id"))
        rows = store.rows(self.MODULE)
        row = next((r for r in rows if str(r.get("报告编号")) == report_id), None)
        values = {
            "报告编号": report_id,
            "关联源": report.get("source_key"),
            "解析成功率": report.get("parse_success_rate"),
            "字段完整性": f"{report.get('valid_raw_lines')}/{report.get('total_records')}",
            "延迟分布": "回溯任务不评估延迟",
            "脏数据类型": json.dumps(report.get("abnormal_categories", {}), ensure_ascii=False),
            "诊断时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(report.get("diagnosed_at") or time.time())),
            "报告状态": report.get("report_status"),
            "report_type": "backfill",
            "task_id": report.get("task_id"),
            "time_range": report.get("time_range"),
            "valid_raw_lines": report.get("valid_raw_lines"),
            "bad_lines": report.get("bad_lines"),
            "range_proof": report.get("range_proof"),
            "detail": report,
        }
        if row is None:
            row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            row.update(values)
            row["status"] = "已生成"
            row["pending"] = False
            row["abnormal"] = bool(report.get("bad_lines"))
            rows.append(row)
            return
        row.update(values)
        row["abnormal"] = bool(report.get("bad_lines"))

    def get_backfill_report_failure(self, task_id: str) -> dict[str, Any] | None:
        """读取最近一次回溯质量报告的失败原因（未失败过则返回 None）。"""
        cp = self._read_checkpoint(f"backfill_quality_{task_id}") or {}
        task_cp = self._read_checkpoint(f"backfill_{task_id}") or {}
        if cp.get("report_status") != "失败" and task_cp.get("quality_report_status") != "失败":
            return None
        return {
            "task_id": task_id,
            "source_key": cp.get("source_key") or task_cp.get("source_key"),
            "time_range": cp.get("time_range") or task_cp.get("time_range"),
            "report_status": "失败",
            "error": cp.get("failure_reason")
            or task_cp.get("quality_failure_reason")
            or "质量报告生成失败",
            "failed_at": cp.get("failed_at") or task_cp.get("quality_failed_at"),
        }

    def get_backfill_report_detail(self, task_id: str) -> dict[str, Any] | None:
        """质量报告详情：优先返回已确认的任务报告，失败时返回失败说明。"""
        report = self.get_backfill_quality_report(task_id)
        if report is None:
            return self.get_backfill_report_failure(task_id)
        detail = dict(report)
        detail["anomalies"] = self.get_backfill_anomalies(task_id)
        return detail

    # 别名：保持调用方可以按业务语义查找 --------------------------------

    def generate_backfill_quality_report(self, task_id: str, source_key: str | None = None,
                                         time_range: str | None = None, force: bool = False) -> dict[str, Any]:
        return self.diagnose_backfill_quality(task_id, source_key, time_range, force)

    def diagnose_backfill(self, task_id: str, source_key: str | None = None,
                          time_range: str | None = None, force: bool = False) -> dict[str, Any]:
        return self.diagnose_backfill_quality(task_id, source_key, time_range, force)

    def _compute_completeness(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        if not records:
            return {"rate": 100, "total": 0, "description": "无记录"}

        field_counts = defaultdict(lambda: {"present": 0, "total": 0})
        for rec in records:
            fields = rec.get("fields", rec)
            for k, v in fields.items():
                field_counts[k]["total"] += 1
                if v is not None and v != "":
                    field_counts[k]["present"] += 1

        result = {"fields": {}}
        for field, counts in field_counts.items():
            rate = round(counts["present"] / max(counts["total"], 1) * 100, 2)
            result["fields"][field] = rate

        rates = [v for v in result["fields"].values()]
        result["overall"] = round(sum(rates) / max(len(rates), 1), 2) if rates else 100
        return result

    def _compute_latency_distribution(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        latencies = []
        for rec in records:
            fields = rec.get("fields", rec)
            for k in ("latency_ms", "latency", "duration_ms"):
                val = fields.get(k)
                if val is not None:
                    try:
                        latencies.append(float(val))
                    except (ValueError, TypeError):
                        pass

        if not latencies:
            return {"count": 0, "description": "无延迟数据"}

        return {
            "count": len(latencies),
            "min": round(min(latencies), 2),
            "max": round(max(latencies), 2),
            "avg": round(statistics.mean(latencies), 2),
            "p50": round(statistics.median(latencies), 2),
            "p95": round(self._percentile(latencies, 95), 2),
            "p99": round(self._percentile(latencies, 99), 2),
        }

    def _percentile(self, data: list[float], percentile: float) -> float:
        if not data:
            return 0
        sorted_data = sorted(data)
        index = (percentile / 100.0) * (len(sorted_data) - 1)
        lower = int(math.floor(index))
        upper = int(math.ceil(index))
        if lower == upper:
            return sorted_data[lower]
        weight = index - lower
        return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight

    def _classify_dirty_data(self, error_records: list[dict[str, Any]]) -> dict[str, Any]:
        types = defaultdict(int)
        for rec in error_records:
            error_msg = rec.get("parse_error", "unknown")
            if "正则" in str(error_msg):
                types["regex_mismatch"] += 1
            elif "格式" in str(error_msg):
                types["format_error"] += 1
            elif "字段" in str(error_msg):
                types["missing_field"] += 1
            else:
                types["other"] += 1
        return {"total": len(error_records), "categories": dict(types)}

    def compare_quality_trend(self, source_key: str) -> dict[str, Any]:
        """对比最近两次质量诊断的趋势。"""
        current = self._read_checkpoint(f"quality_{source_key}")
        reports = self._read_jsonl(self._data_dir("exports") / "quality_reports.jsonl")
        source_reports = [r for r in reports if r.get("source_key") == source_key]

        if len(source_reports) < 2:
            return {"source_key": source_key, "status": "需要至少两次诊断才能对比趋势"}

        latest = source_reports[-1]
        previous = source_reports[-2]

        return {
            "source_key": source_key,
            "latest_success_rate": latest.get("parse_success_rate", 0),
            "previous_success_rate": previous.get("parse_success_rate", 0),
            "trend": "up" if latest.get("parse_success_rate", 0) > previous.get("parse_success_rate", 0) else "down" if latest.get("parse_success_rate", 0) < previous.get("parse_success_rate", 0) else "stable",
            "delta": round(latest.get("parse_success_rate", 0) - previous.get("parse_success_rate", 0), 2),
        }