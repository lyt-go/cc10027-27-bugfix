"""模块服务基类：共享查询、登记、动作、文件IO和跨模块数据流骨架。"""
from __future__ import annotations

import json
import os
import hashlib
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from app.store import store

DATA_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "data"

_TIME_RANGE_SPLIT = re.compile(r"\s*(?:~|～|至|—|–|--|,|，)\s*")
_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y%m%d",
)


def parse_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_time_bounds(time_range: str) -> tuple[datetime, datetime] | None:
    """把 ``2026-09-01~2026-09-30`` 之类的时间范围解析为闭区间。"""
    text = str(time_range or "").strip()
    if not text:
        return None
    parts = [part for part in _TIME_RANGE_SPLIT.split(text) if part.strip()]
    if len(parts) == 1:
        start = parse_datetime(parts[0])
        if start is None:
            return None
        return start, start.replace(hour=23, minute=59, second=59, microsecond=999999)
    if len(parts) >= 2:
        start = parse_datetime(parts[0])
        end = parse_datetime(parts[1])
        if start is None or end is None:
            return None
        if len(parts[1].strip()) <= 10:
            end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        if end < start:
            start, end = end, start
        return start, end
    return None


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # -- 基础 CRUD --

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }

    # -- 文件 IO 工具 --

    def _data_dir(self, sub: str) -> Path:
        p = DATA_ROOT / sub
        _ensure_dir(p)
        return p

    def _write_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        _ensure_dir(filepath.parent)
        written = 0
        with open(filepath, "a+", encoding="utf-8") as f:
            # a+ 直接追加；若上次写入被中断留下没有换行的半行，先补换行，
            # 避免把新记录粘在坏行末尾。
            if filepath.stat().st_size > 0:
                f.seek(0, os.SEEK_END)
                f.seek(f.tell() - 1)
                if f.read(1) != "\n":
                    f.write("\n")
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                written += 1
        return written

    def _read_jsonl(self, filepath: Path) -> list[dict[str, Any]]:
        if not filepath.exists():
            return []
        records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def _atomic_write_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """整体覆盖写入 JSONL：先写临时文件再原子替换，避免中断留下半份内容。

        临时文件的写入复用 ``_write_jsonl``，这样外部替换写实现（或注入写入失败）
        时，报告写入路径也会一并生效。
        """
        _ensure_dir(filepath.parent)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(filepath.parent), prefix=f".{filepath.name}.", suffix=".tmp")
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            written = self._write_jsonl(tmp_path, records)
            os.replace(tmp_name, filepath)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
        return written

    def _atomic_write_json(self, filepath: Path, payload: Any) -> Path:
        """整体覆盖写入 JSON：临时文件 + 原子替换，中断时保留旧版本。"""
        _ensure_dir(filepath.parent)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(filepath.parent), prefix=f".{filepath.name}.", suffix=".tmp")
        os.close(fd)
        try:
            self._write_jsonl(Path(tmp_name), [payload])
            os.replace(tmp_name, filepath)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
        return filepath

    def _write_checkpoint(self, name: str, state: dict[str, Any]) -> Path:
        p = self._data_dir("checkpoints")
        cp_file = p / f"{name}.json"
        state["_updated_at"] = time.time()
        return self._atomic_write_json(cp_file, state)

    def _read_checkpoint(self, name: str) -> dict[str, Any] | None:
        cp_file = self._data_dir("checkpoints") / f"{name}.json"
        if not cp_file.exists():
            return None
        try:
            with open(cp_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
        return data if isinstance(data, dict) else None

    def _compute_checksum(self, content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def _collect_sources(self) -> list[dict[str, Any]]:
        """从 store 和文件系统中收集所有活跃的日志源。"""
        sources = [r for r in store.rows("log_source") if r.get("status") == "已接入"]
        return sources

    def _get_parsers_for_source(self, source_id: int) -> list[dict[str, Any]]:
        parsers = store.rows("log_parser")
        source_code = f"LOG_-{source_id:04d}"
        return [p for p in parsers if p.get("关联源", "").find(source_code) >= 0 or str(p.get("关联源")) == str(source_id)]

    def _get_mappings_for_source(self, source_id: int) -> list[dict[str, Any]]:
        mappings = store.rows("field_mapping")
        return [m for m in mappings if m.get("status") == "已生效"]

    def _simulate_log_lines(self, source: dict[str, Any], count: int = 10) -> list[str]:
        """模拟生成日志行用于测试数据流。"""
        lines = []
        source_name = source.get("源名称", "unknown")
        for i in range(count):
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - i * 3600))
            lines.append(
                f"{timestamp} [{source_name}] INFO event_id={i} "
                f"source={source.get('源编号', 'N/A')} "
                f"msg=sample_log_line_{i} latency_ms={50 + i * 7}"
            )
        return lines

    def _notify_alert(self, rule_id: int, level: str, message: str, source_key: str = "") -> dict[str, Any]:
        """触发告警通知。"""
        alert = {
            "rule_id": rule_id,
            "level": level,
            "message": message,
            "source_key": source_key,
            "fired_at": time.time(),
            "acknowledged": False,
        }
        alerts = self._read_jsonl(self._data_dir("exports") / "alerts.jsonl")
        alerts.append(alert)
        self._write_checkpoint("alert_state", {"last_fired": alert["fired_at"], "total": len(alerts)})
        return alert

    def _rollback_checkpoint(self, source_key: str, to_offset: int) -> dict[str, Any]:
        """将指定位点回滚到指定偏移。"""
        state = self._read_checkpoint(source_key) or {"source_key": source_key, "offset": 0, "status": "active"}
        state["offset"] = to_offset
        state["status"] = "rolled_back"
        state["rolled_back_at"] = time.time()
        self._write_checkpoint(source_key, state)
        return state

    def _run_cross_pipeline(self, pipeline_name: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
        """通用跨模块管道运行入口。返回管道产物摘要。"""
        results = {
            "pipeline": pipeline_name,
            "started_at": time.time(),
            "sources_processed": len(sources),
            "records_written": 0,
            "errors": 0,
            "stages": {},
        }
        return results

    # -- 跨模块自动触发帮助器 --

    @staticmethod
    def _safe_import(module_path: str, class_name: str):
        """延迟导入，避免循环依赖。"""
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)

    def _auto_create_seed(self, module: str, overrides: dict[str, Any]) -> dict[str, Any] | None:
        """跨模块自动创建配套配置条目。"""
        from app.store import store
        rows = store.rows(module)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        entry = {"id": new_id}
        for field, default_val in overrides.items():
            entry[field] = default_val
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False
        rows.append(entry)
        return entry

    def _trigger_parse_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发日志解析。"""
        try:
            cls = self._safe_import("app.services.log_parser", "LogParserService")
            return cls().parse_log_file(source_key)
        except Exception as e:
            return {"auto_trigger": "parse", "source_key": source_key, "error": str(e)}

    def _trigger_mapping_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发现场映射。"""
        try:
            cls = self._safe_import("app.services.field_mapping", "FieldMappingService")
            return cls().apply_mappings(source_key)
        except Exception as e:
            return {"auto_trigger": "mapping", "source_key": source_key, "error": str(e)}

    def _trigger_export_for_source(self, source_key: str, fmt: str = "jsonl") -> dict[str, Any]:
        """自动触发格式输出。"""
        try:
            cls = self._safe_import("app.services.format_output", "FormatOutputService")
            return cls().export_source(source_key, fmt)
        except Exception as e:
            return {"auto_trigger": "export", "source_key": source_key, "error": str(e)}

    def _trigger_quality_diagnosis(self, source_key: str) -> dict[str, Any]:
        """自动触发质量诊断（在线采集数据）。"""
        try:
            cls = self._safe_import("app.services.log_quality", "LogQualityService")
            return cls().diagnose_source(source_key)
        except Exception as e:
            return {"auto_trigger": "quality", "source_key": source_key, "error": str(e)}

    def _trigger_backfill_quality_diagnosis(
        self, task_id: str, source_key: str | None = None
    ) -> dict[str, Any]:
        """自动触发回溯质量诊断：只读取该回溯任务实际保存的采集明细。"""
        try:
            cls = self._safe_import("app.services.log_quality", "LogQualityService")
            return cls().diagnose_backfill_quality(task_id, source_key=source_key)
        except Exception as e:
            return {
                "auto_trigger": "backfill_quality",
                "task_id": task_id,
                "source_key": source_key or "",
                "error": str(e),
            }

    def _trigger_alert_evaluation(self, source_key: str, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
        """自动触发告警评估。"""
        try:
            cls = self._safe_import("app.services.alert_rule", "AlertRuleService")
            return cls().evaluate_rules(source_key, metrics)
        except Exception as e:
            return {"auto_trigger": "alert", "source_key": source_key, "error": str(e)}

    def _trigger_checkpoint_rollback(self, source_key: str, to_offset: int) -> dict[str, Any]:
        """自动触发位点回滚。"""
        try:
            cls = self._safe_import("app.services.collect_checkpoint", "CollectCheckpointService")
            return cls().rollback_to(source_key, to_offset)
        except Exception as e:
            return {"auto_trigger": "rollback", "source_key": source_key, "error": str(e)}

    def _trigger_dedup_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发窗口去重。"""
        try:
            cls = self._safe_import("app.services.dedup_window", "DedupWindowService")
            return cls().deduplicate_source(source_key)
        except Exception as e:
            return {"auto_trigger": "dedup", "source_key": source_key, "error": str(e)}

    def _trigger_sampling_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发采样。"""
        try:
            cls = self._safe_import("app.services.sampling_rule", "SamplingRuleService")
            return cls().sample_source(source_key)
        except Exception as e:
            return {"auto_trigger": "sampling", "source_key": source_key, "error": str(e)}

    def _trigger_validation_for_event(self, event_code: str) -> dict[str, Any]:
        """自动触发事件校验。"""
        try:
            cls = self._safe_import("app.services.event_validate", "EventValidateService")
            return cls().validate_event_data(event_code)
        except Exception as e:
            return {"auto_trigger": "validate", "event_code": event_code, "error": str(e)}

    def _trigger_rotate_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发日志轮转。"""
        try:
            cls = self._safe_import("app.services.log_rotate", "LogRotateService")
            return cls().rotate_source(source_key)
        except Exception as e:
            return {"auto_trigger": "rotate", "source_key": source_key, "error": str(e)}

    def _trigger_partition_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发日志分区。"""
        try:
            cls = self._safe_import("app.services.log_partition", "LogPartitionService")
            return cls().partition_source(source_key)
        except Exception as e:
            return {"auto_trigger": "partition", "source_key": source_key, "error": str(e)}

    def _trigger_enrich_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发维表富化。"""
        try:
            cls = self._safe_import("app.services.enrich_join", "EnrichJoinService")
            return cls().enrich_source(source_key)
        except Exception as e:
            return {"auto_trigger": "enrich", "source_key": source_key, "error": str(e)}

    def _trigger_aggregate_for_source(self, source_key: str) -> dict[str, Any]:
        """自动触发聚合计算。"""
        try:
            cls = self._safe_import("app.services.aggregate_olap", "AggregateOlapService")
            return cls().aggregate_source(source_key)
        except Exception as e:
            return {"auto_trigger": "aggregate", "source_key": source_key, "error": str(e)}

    def _has_auto_triggers(self) -> bool:
        """子类可覆盖以声明自己支持自动触发。"""
        return False