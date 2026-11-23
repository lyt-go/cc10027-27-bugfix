"""回溯采集：管理历史数据的回溯采集任务，支持时间范围、断点续做和失败重试。

回溯任务的采集明细按任务单独落盘（``<source>_backfill_<task>.jsonl``），
同时保留源级镜像 ``<source>_backfill.jsonl`` 供旧界面查看。质量诊断只读取
任务明细（或按 task_id 过滤后的旧文件），不会把在线采集或同源其它任务混入。
"""
from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService, parse_time_bounds
from app.store import store

# 回溯明细 raw 行的规范格式；质量诊断按这个格式校验成功行/异常行。
BACKFILL_LINE_RE = re.compile(
    r"^(?P<timestamp>\S+) \[(?P<stream>backfill)\] (?P<level>[A-Z]+) "
    r"source=(?P<source>\S+) task=(?P<task>\S+) record_id=(?P<record_id>\d+)"
    r"(?: batch=(?P<batch>\d+))?(?: (?P<message>.*))?$"
)


class BackfillRangeService(BaseModuleService):
    MODULE = "backfill_range"
    ENTITY = "回溯任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "关联源", "时间范围", "数据量估计", "并发度", "跳过空文件", "校验策略", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "关联源", "时间范围"]
    STATUS_ORDER = ["排队中", "采集", "已完成", "已失败"]
    ACTION_RULES = {"发起采集": "采集", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    # 明细 / 范围记录相关路径 -------------------------------------------------

    def _task_detail_path(self, source_key: str, task_id: str) -> Path:
        """任务级明细文件：同一源的不同时间段互不影响。"""
        return self._data_dir("logs") / f"{source_key}_backfill_{task_id}.jsonl"

    def _legacy_backfill_path(self, source_key: str) -> Path:
        """源级镜像：仅供人工查看，不作为质量诊断输入。"""
        return self._data_dir("logs") / f"{source_key}_backfill.jsonl"

    def _history_path(self) -> Path:
        return self._data_dir("logs") / "backfill_history.jsonl"

    def _find_task_row(self, task_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if str(row.get("任务编号")) == task_id:
                return row
        return None

    def _next_task_id(self) -> str:
        """保证同一秒内创建的两个时间段任务也拿到不同任务编号。"""
        base = f"BACK-{int(time.time())}"
        existing = {str(row.get("任务编号")) for row in store.rows(self.MODULE)}
        for cp in self._data_dir("checkpoints").glob("backfill_BACK-*.json"):
            existing.add(cp.stem[len("backfill_"):])
        candidate = base
        seq = 2
        while candidate in existing:
            candidate = f"{base}-{seq}"
            seq += 1
        return candidate

    def _record_timestamp(self, bounds: tuple[datetime, datetime] | None, record_no: int) -> datetime:
        """把记录时间落在任务时间范围内（无法解析范围时退回 30 天前）。"""
        if bounds:
            start, end = bounds
            span = max(int((end - start).total_seconds()), 1)
            offset = ((record_no - 1) * 60) % span
            return start + timedelta(seconds=offset)
        return datetime.fromtimestamp(time.time() - 86400 * 30 + (record_no % 240) * 3600)

    def _append_task_detail(self, path: Path, records: list[dict[str, Any]]) -> int:
        """追加任务明细，整文件原子重写，避免中断后留下半行。"""
        existing = self._read_jsonl(path)
        combined = existing + records
        self._atomic_write_jsonl(path, combined)
        return len(records)

    # 任务创建 / 采集 --------------------------------------------------------

    def create_backfill(self, source_key: str, time_range: str, concurrency: int = 1) -> dict[str, Any]:
        """创建回溯采集任务，并为任务绑定独立明细文件。"""
        task_id = self._next_task_id()
        detail_file = self._task_detail_path(source_key, task_id)
        entry, _ = self.create_entry({
            "任务编号": task_id,
            "关联源": source_key,
            "时间范围": time_range,
            "并发度": str(concurrency),
            "数据量估计": "待计算",
            "跳过空文件": "是",
            "校验策略": "完整性校验",
            "任务状态": "排队中",
        })
        self._write_checkpoint(f"backfill_{task_id}", {
            "task_id": task_id,
            "source_key": source_key,
            "time_range": time_range,
            "status": "created",
            "created_at": time.time(),
            "offset": 0,
            "line_no": 0,
            "record_count": 0,
            "batches": 0,
            "detail_file": str(detail_file),
            "input_file": str(detail_file),
            "history_recorded": False,
            "quality_report_id": None,
            "quality_report_status": None,
            "quality_failure_reason": None,
        })
        return entry or {}

    def execute_backfill(self, task_id: str, batch_size: int = 50) -> dict[str, Any]:
        """执行一批回溯采集，只追加到本任务的明细文件。"""
        task = self._find_task_row(task_id)
        cp = self._read_checkpoint(f"backfill_{task_id}")
        if task is None and cp is None:
            return {"error": f"回溯任务 {task_id} 不存在", "task_id": task_id, "written": 0}

        source_key = (cp or {}).get("source_key") or (task or {}).get("关联源", "")
        time_range = (cp or {}).get("time_range") or (task or {}).get("时间范围", "")
        if not source_key:
            return {"error": f"回溯任务 {task_id} 缺少关联源", "task_id": task_id, "written": 0}

        cp = cp or {
            "task_id": task_id, "source_key": source_key, "time_range": time_range,
            "offset": 0, "line_no": 0, "record_count": 0, "batches": 0, "created_at": time.time(),
        }
        batch_no = int(cp.get("batches", 0)) + 1
        start_no = int(cp.get("line_no", 0))
        bounds = parse_time_bounds(time_range)

        records: list[dict[str, Any]] = []
        for i in range(max(int(batch_size), 0)):
            record_no = start_no + i + 1
            ts = self._record_timestamp(bounds, record_no)
            ts_text = ts.strftime("%Y-%m-%dT%H:%M:%S")
            raw = (
                f"{ts_text} [backfill] INFO source={source_key} task={task_id} "
                f"record_id={record_no} batch={batch_no}"
            )
            records.append({
                "source_key": source_key,
                "task_id": task_id,
                "time_range": time_range,
                "batch": batch_no,
                "record_id": record_no,
                "line_no": record_no,
                "offset": int(cp.get("offset", 0)) + i,
                "ts": ts_text,
                "raw": raw,
                "backfilled_at": time.time(),
            })

        if not records:
            return {"task_id": task_id, "written": 0, "total_lines": start_no, "offset": cp.get("offset", 0)}

        detail_file = self._task_detail_path(source_key, task_id)
        written = self._append_task_detail(detail_file, records)
        # 旧镜像文件保留追加，方便人工查看；质量诊断不会读它。
        try:
            self._write_jsonl(self._legacy_backfill_path(source_key), records)
        except OSError:
            pass

        cp["detail_file"] = str(detail_file)
        cp["input_file"] = str(detail_file)
        cp["offset"] = int(cp.get("offset", 0)) + sum(len(r["raw"].encode("utf-8")) + 1 for r in records)
        cp["line_no"] = start_no + written
        cp["record_count"] = cp["line_no"]
        cp["batches"] = batch_no
        cp["status"] = "in_progress"
        cp["last_batch_at"] = time.time()
        cp["detail_sha256"] = self._file_sha256(detail_file)
        self._write_checkpoint(f"backfill_{task_id}", cp)

        if task is not None:
            task["status"] = "采集"
            task["任务状态"] = "采集中"
            task["数据量估计"] = f"{cp['line_no']} 行"

        return {
            "task_id": task_id,
            "source_key": source_key,
            "written": written,
            "total_lines": cp["line_no"],
            "offset": cp["offset"],
            "detail_file": str(detail_file),
        }

    # 完成 / 历史 ------------------------------------------------------------

    def _record_completion_history(
        self, task_id: str, source_key: str, time_range: str, cp: dict[str, Any]
    ) -> dict[str, Any] | None:
        """完成入口写入历史日志；重试时按 checkpoint 标记幂等，不重复追加。"""
        if cp.get("history_recorded"):
            return None
        entry = {
            "task_id": task_id,
            "source_key": source_key,
            "time_range": time_range,
            "action": "complete",
            "status": "completed",
            "detail_file": cp.get("detail_file", ""),
            "record_count": int(cp.get("line_no", 0)),
            "offset": cp.get("offset", 0),
            "batches": cp.get("batches", 0),
            "completed_at": time.time(),
        }
        self._write_jsonl(self._history_path(), [entry])
        cp["history_recorded"] = True
        cp["history_logged_at"] = entry["completed_at"]
        return entry

    def complete_backfill(self, task_id: str) -> dict[str, Any]:
        """完成回溯任务，写入历史日志，并生成任务独立的回溯质量报告。"""
        cp = self._read_checkpoint(f"backfill_{task_id}")
        task = self._find_task_row(task_id)
        if cp is None and task is None:
            return {
                "error": f"回溯任务 {task_id} 不存在，无法完成",
                "task_id": task_id,
                "status": "failed",
                "diagnosed": False,
            }

        source_key = (cp or {}).get("source_key") or (task or {}).get("关联源", "")
        time_range = (cp or {}).get("time_range") or (task or {}).get("时间范围", "")
        cp = cp or {
            "task_id": task_id, "source_key": source_key, "time_range": time_range,
            "offset": 0, "line_no": 0, "batches": 0, "created_at": time.time(),
        }
        cp["source_key"] = source_key
        cp["time_range"] = time_range
        cp["status"] = "completed"
        cp["completed_at"] = time.time()
        if not cp.get("detail_file") and source_key:
            cp["detail_file"] = str(self._task_detail_path(source_key, task_id))
        self._write_checkpoint(f"backfill_{task_id}", cp)

        if task is not None:
            task["status"] = "已完成"
            task["任务状态"] = "已完成"
            task["pending"] = False
            task["数据量估计"] = f"{int(cp.get('line_no', 0))} 行"

        history_entry = self._record_completion_history(task_id, source_key, time_range, cp)
        self._write_checkpoint(f"backfill_{task_id}", cp)

        result: dict[str, Any] = {
            "task_id": task_id,
            "source_key": source_key,
            "time_range": time_range,
            "status": "completed",
            "history": history_entry,
            "result": cp,
        }

        if not source_key:
            result["quality_error"] = {
                "error": f"回溯任务 {task_id} 缺少关联源，无法生成质量报告",
                "task_id": task_id,
                "report_status": "失败",
                "diagnosed": False,
            }
            return result

        # 关键修复：回溯质量诊断绑定 source + task，只读本任务实际采得的明细。
        quality_r = self._trigger_backfill_quality_diagnosis(task_id, source_key)
        quality_failed = "error" in quality_r or quality_r.get("report_status") == "失败"

        # 先回填本次尝试的行数信息：失败时也要能解释发现了多少有效行/坏行。
        if quality_failed:
            if quality_r.get("valid_raw_lines") is not None:
                cp["quality_valid_raw_lines"] = quality_r.get("valid_raw_lines")
            if quality_r.get("bad_lines") is not None:
                cp["quality_bad_lines"] = quality_r.get("bad_lines")
            if quality_r.get("batch_key"):
                cp["quality_batch_key"] = quality_r.get("batch_key")

        if quality_failed:
            result["quality_error"] = {
                "error": quality_r.get("error") or "回溯质量报告生成失败",
                "task_id": task_id,
                "source_key": source_key,
                "time_range": time_range,
                "report_status": "失败",
                "diagnosed": False,
                "batch_key": quality_r.get("batch_key"),
                "valid_raw_lines": quality_r.get("valid_raw_lines"),
                "bad_lines": quality_r.get("bad_lines"),
            }
            result["message"] = f"回溯任务已完成，但质量报告生成失败：{result['quality_error']['error']}"
            cp["quality_report_status"] = "失败"
            cp["quality_failure_reason"] = result["quality_error"]["error"]
            cp["quality_failed_at"] = time.time()
            self._write_checkpoint(f"backfill_{task_id}", cp)
        else:
            result["auto_quality"] = quality_r
            cp["quality_report_id"] = quality_r.get("report_id")
            cp["quality_batch_key"] = quality_r.get("batch_key")
            cp["quality_report_status"] = "已确认"
            cp["quality_failure_reason"] = None
            if quality_r.get("input_file"):
                cp["quality_report_file"] = quality_r.get("input_file")
            if quality_r.get("evidence_file"):
                cp["quality_evidence_file"] = quality_r.get("evidence_file")
            self._write_checkpoint(f"backfill_{task_id}", cp)

        return result

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """回溯完成入口：状态流转后立即生成任务级质量报告。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if self.ACTION_RULES.get(action) == "已完成":
            task_id = str(entry.get("任务编号", ""))
            if task_id:
                completion = self.complete_backfill(task_id)
                if "auto_quality" in completion:
                    entry["quality_report"] = completion["auto_quality"]
                else:
                    quality_error = completion.get("quality_error", {})
                    entry["quality_error"] = quality_error
                    entry["quality_report"] = quality_error
                    message = f"{message}；质量报告生成失败：{quality_error.get('error', '未知原因')}"
        return entry, message

    # 查询辅助 ---------------------------------------------------------------

    def get_quality_report(self, task_id: str) -> dict[str, Any] | None:
        """任务级回溯质量报告详情；失败时返回含失败原因的占位说明。"""
        cls = self._safe_import("app.services.log_quality", "LogQualityService")
        report = cls().get_backfill_quality_report(task_id)
        if report is not None:
            return report
        cp = self._read_checkpoint(f"backfill_{task_id}") or {}
        if cp.get("quality_report_status") == "失败":
            return {
                "task_id": task_id,
                "source_key": cp.get("source_key"),
                "time_range": cp.get("time_range"),
                "report_id": None,
                "report_status": "失败",
                "diagnosed": False,
                "error": cp.get("quality_failure_reason", "质量报告生成失败"),
                "valid_raw_lines": None,
                "bad_lines": None,
            }
        return None

    def diagnose_task_quality(self, task_id: str, force: bool = False) -> dict[str, Any]:
        """对本任务重新生成回溯质量报告（绑定 source + task）。"""
        cp = self._read_checkpoint(f"backfill_{task_id}") or {}
        cls = self._safe_import("app.services.log_quality", "LogQualityService")
        return cls().diagnose_backfill_quality(
            task_id, source_key=cp.get("source_key") or None, force=force)

    def get_quality_anomalies(self, task_id: str) -> list[dict[str, Any]]:
        cls = self._safe_import("app.services.log_quality", "LogQualityService")
        return cls().get_backfill_anomalies(task_id)

    def get_backfill_history(self, source_key: str | None = None, task_id: str | None = None) -> list[dict[str, Any]]:
        """历史回溯完成记录（含异常证据入口）仍可回查。"""
        history = self._read_jsonl(self._history_path())
        if source_key:
            history = [h for h in history if h.get("source_key") == source_key]
        if task_id:
            history = [h for h in history if h.get("task_id") == task_id]
        return history

    def get_task_detail(self, task_id: str, source_key: str | None = None) -> dict[str, Any]:
        """读取任务明细并区分本任务记录、其它任务记录和缺失文件。"""
        cp = self._read_checkpoint(f"backfill_{task_id}") or {}
        task = self._find_task_row(task_id) or {}
        source_key = source_key or cp.get("source_key") or task.get("关联源", "")
        detail_file = Path(str(cp.get("detail_file") or self._task_detail_path(source_key, task_id)))
        is_task_file = detail_file.name == self._task_detail_path(source_key, task_id).name
        if not detail_file.exists():
            legacy = self._legacy_backfill_path(source_key) if source_key else None
            if legacy is not None and legacy.exists():
                detail_file = legacy
                is_task_file = False
            else:
                return {
                    "task_id": task_id,
                    "source_key": source_key,
                    "detail_file": str(detail_file),
                    "found": False,
                    "records": [],
                    "excluded": [],
                }
        records = self._read_jsonl(detail_file)
        bound = []
        excluded = []
        for record in records:
            rec_task = str(record.get("task_id", ""))
            rec_source = str(record.get("source_key", ""))
            matches = (not rec_task or rec_task == task_id) and (
                not rec_source or not source_key or rec_source == source_key)
            if not is_task_file and not rec_task:
                matches = False
            (bound if matches else excluded).append(record)
        return {
            "task_id": task_id,
            "source_key": source_key,
            "detail_file": str(detail_file),
            "detail_is_task_scoped": is_task_file,
            "found": True,
            "records": bound,
            "excluded": excluded,
        }

    def _file_sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
