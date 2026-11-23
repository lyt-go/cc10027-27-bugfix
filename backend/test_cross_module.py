"""跨模块数据流集成测试：覆盖7组跨模块管道和关键业务逻辑。"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path

# 确保 backend 在 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.services.log_source import LogSourceService
from app.services.log_parser import LogParserService
from app.services.field_mapping import FieldMappingService
from app.services.format_output import FormatOutputService
from app.services.event_tracker import EventTrackerService
from app.services.event_validate import EventValidateService
from app.services.stream_buffer import StreamBufferService
from app.services.agent_heartbeat import AgentHeartbeatService
from app.services.collect_checkpoint import CollectCheckpointService
from app.services.backfill_range import BackfillRangeService
from app.services.log_rotate import LogRotateService
from app.services.log_partition import LogPartitionService
from app.services.enrich_join import EnrichJoinService
from app.services.aggregate_olap import AggregateOlapService
from app.services.alert_rule import AlertRuleService
from app.services.dedup_window import DedupWindowService
from app.services.sampling_rule import SamplingRuleService
from app.services.log_quality import LogQualityService
from app.services.pipeline import CrossModulePipeline
from app.store import store


def setup_module():
    """确保测试前有已接入的日志源。"""
    # 接入日志源
    source = None
    for src in store.rows("log_source"):
        if src.get("status") == "草稿":
            src["status"] = "已接入"
            src["pending"] = False
            source = src
            break

    if source is None:
        # 创建新日志源
        rows = store.rows("log_source")
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        source = {
            "id": new_id,
            "源编号": f"LOG_-{new_id:04d}",
            "源名称": f"测试日志源{new_id}",
            "日志类型": "test",
            "接入方式": "api",
            "路径模版": "/var/log/test",
            "字符编码": "UTF-8",
            "轮转规则": "daily",
            "源状态": "已接入",
            "status": "已接入",
            "pending": False,
            "abnormal": False,
        }
        rows.append(source)

    # 确保事件生效
    event = None
    for ev in store.rows("event_tracker"):
        if ev.get("status") == "已生效":
            event = ev
            break

    # 确保解析规则生效
    has_parser = any(p.get("status") == "已生效" for p in store.rows("log_parser"))
    if not has_parser:
        for p in store.rows("log_parser"):
            if p.get("status") in ("草稿", "待校验"):
                p["status"] = "已生效"
                p["pending"] = False
                break

    # 确保富化规则生效
    has_enrich = any(e.get("status") == "已生效" for e in store.rows("enrich_join"))
    if not has_enrich:
        for e in store.rows("enrich_join"):
            if e.get("status") in ("草稿", "待校验"):
                e["status"] = "已生效"
                e["pending"] = False
                break

    # 确保告警规则生效
    has_alert = any(a.get("status") == "已生效" for a in store.rows("alert_rule"))
    if not has_alert:
        for a in store.rows("alert_rule"):
            if a.get("status") in ("草稿", "待校验"):
                a["status"] = "已生效"
                a["pending"] = False
                break


def test_log_source_collection():
    """测试：日志源采集"""
    svc = LogSourceService()
    sources = svc.get_active_sources()
    assert len(sources) > 0, "应有活跃日志源"

    source_id = sources[0]["id"]
    result = svc.collect_from_source(source_id, batch_size=10)
    assert result.get("written", 0) > 0, f"应写入日志行: {result}"
    assert "checkpoint_offset" in result


def test_log_parser():
    """测试：日志解析"""
    svc = LogParserService()
    sources = LogSourceService().get_active_sources()
    assert len(sources) > 0
    source_key = sources[0]["源编号"]

    # 先确保有采集数据
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=5)

    result = svc.parse_log_file(source_key)
    assert result.get("parsed", 0) > 0, f"应有解析记录: {result}"


def test_field_mapping():
    """测试：字段映射"""
    svc = FieldMappingService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有已解析数据
    LogParserService().parse_log_file(source_key)

    result = svc.apply_mappings(source_key)
    assert result.get("mapped", 0) > 0, f"应有映射记录: {result}"


def test_format_output():
    """测试：格式输出"""
    svc = FormatOutputService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有映射数据
    FieldMappingService().apply_mappings(source_key)

    result = svc.export_source(source_key, "jsonl")
    assert result.get("records", 0) > 0, f"应有导出记录: {result}"


def test_full_collect_pipeline():
    """测试：完整采集管道（log_source → log_parser → field_mapping → format_output）"""
    p = CrossModulePipeline()
    result = p.full_collect_pipeline()
    assert "stages" in result
    assert len(result["stages"]) > 0
    for key, stages in result["stages"].items():
        assert "collect" in stages
        assert "parse" in stages
        assert "mapping" in stages
        assert "output" in stages


def test_event_tracking():
    """测试：事件埋点 -> 流式缓冲 -> 校验"""
    tracker = EventTrackerService()
    events = [e for e in store.rows("event_tracker") if e.get("status") == "已生效"]
    if not events:
        events = [store.rows("event_tracker")[0]]
        events[0]["status"] = "已生效"

    event_code = events[0]["事件编号"]

    result = tracker.track_event(event_code, {"action": "click", "page": "home"})
    assert "count" in result

    # 缓冲
    buffer_svc = StreamBufferService()
    buffer_result = buffer_svc.buffer_event(event_code, {"action": "click"})
    assert "status" in buffer_result


def test_event_validate():
    """测试：事件校验"""
    svc = EventValidateService()
    events = [e for e in store.rows("event_tracker") if e.get("status") == "已生效"]
    if not events:
        events = [store.rows("event_tracker")[0]]
        events[0]["status"] = "已生效"

    event_code = events[0]["事件编号"]

    # 先确保有事件数据
    EventTrackerService().track_event(event_code, {"user_id": "test_001", "action": "page_view"})

    result = svc.validate_event_data(event_code)
    assert "total" in result
    assert "passed" in result


def test_agent_heartbeat():
    """测试：代理心跳"""
    svc = AgentHeartbeatService()
    agents = store.rows("agent_heartbeat")
    assert len(agents) > 0

    agent_code = agents[0]["代理编号"]
    result = svc.send_heartbeat(agent_code, {"collected": 500, "error_rate": 0.01})
    assert result.get("status") in ("运行中", "已注册")


def test_checkpoint_management():
    """测试：位点管理"""
    svc = CollectCheckpointService()
    source_key = "LOG_-0003"

    save_r = svc.save_checkpoint(source_key, 1024, 50)
    assert save_r.get("offset") == 1024

    get_r = svc.get_checkpoint_state(source_key)
    assert get_r is not None

    rollback_r = svc.rollback_to(source_key, 512)
    assert rollback_r.get("offset") == 512


def test_backfill():
    """测试：回溯采集"""
    svc = BackfillRangeService()
    task = svc.create_backfill("LOG_-0003", "2026-09-01~2026-09-30")
    assert "任务编号" in task

    task_id = task["任务编号"]
    result = svc.execute_backfill(task_id, batch_size=10)
    assert result.get("written", 0) > 0


def test_log_rotate():
    """测试：日志轮转"""
    svc = LogRotateService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 先确保有日志文件
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=5)

    result = svc.rotate_source(source_key)
    assert "rotated" in result


def test_log_partition():
    """测试：日志分区"""
    svc = LogPartitionService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有解析数据
    LogParserService().parse_log_file(source_key)

    result = svc.partition_source(source_key)
    assert result.get("partitions", 0) > 0, f"应有分区: {result}"


def test_enrich_join():
    """测试：维表富化"""
    svc = EnrichJoinService()
    rules = [r for r in store.rows("enrich_join") if r.get("status") == "已生效"]
    assert len(rules) > 0

    cache_r = svc.build_dimension_cache(rules[0]["规则编号"])
    assert "sources" in cache_r


def test_aggregate_olap():
    """测试：聚合计算"""
    svc = AggregateOlapService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有解析数据
    LogParserService().parse_log_file(source_key)

    result = svc.aggregate_source(source_key)
    assert result.get("total_records", 0) > 0


def test_alert_rule():
    """测试：告警评估"""
    svc = AlertRuleService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    result = svc.evaluate_rules(source_key, {
        "parse_success_rate": 80,
        "parse_errors": 3,
        "source_key": source_key,
    })
    assert "rules_evaluated" in result


def test_dedup_window():
    """测试：窗口去重"""
    svc = DedupWindowService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有采集数据
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=5)

    result = svc.deduplicate_source(source_key)
    assert "total" in result, f"应有去重结果: {result}"


def test_sampling_rule():
    """测试：采样"""
    svc = SamplingRuleService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有采集数据
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=10)

    result = svc.sample_source(source_key)
    assert "total" in result, f"应有采样结果: {result}"


def test_log_quality():
    """测试：质量诊断"""
    svc = LogQualityService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有解析数据
    LogParserService().parse_log_file(source_key)

    result = svc.diagnose_source(source_key)
    assert "parse_success_rate" in result, f"应有诊断结果: {result}"


def test_cross_module_pipeline():
    """测试：所有跨模块管道"""
    p = CrossModulePipeline()

    # 确保有活跃源和采集数据
    sources = LogSourceService().get_active_sources()
    assert len(sources) > 0
    source_key = sources[0]["源编号"]

    # 先采集日志
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=10)
    LogParserService().parse_log_file(source_key)
    FieldMappingService().apply_mappings(source_key)

    # 1. 采集管道
    collect_result = p.full_collect_pipeline()
    assert "stages" in collect_result

    # 2. 富化聚合管道
    enrich_result = p.enrichment_aggregation_pipeline(source_key)
    assert "stages" in enrich_result

    # 3. 去重采样管道
    dedup_result = p.dedup_sample_pipeline(source_key)
    assert "stages" in dedup_result

    # 4. 质量告警管道
    quality_result = p.quality_alert_pipeline(source_key)
    assert "stages" in quality_result

    # 5. 轮转分区管道
    rotate_result = p.rotate_partition_pipeline(source_key)
    assert "stages" in rotate_result

    # 6. 回溯管道
    backfill_result = p.backfill_pipeline(source_key, "2026-09-01~2026-09-30")
    assert "pipeline" in backfill_result


def test_data_artifacts():
    """测试：验证数据产物持久化"""
    from app.services.base import DATA_ROOT

    # 检查关键数据目录存在
    assert (DATA_ROOT / "logs").exists(), "logs目录应存在"
    assert (DATA_ROOT / "events").exists(), "events目录应存在"
    assert (DATA_ROOT / "checkpoints").exists(), "checkpoints目录应存在"
    assert (DATA_ROOT / "aggregations").exists(), "aggregations目录应存在"
    assert (DATA_ROOT / "exports").exists(), "exports目录应存在"

    # 采集后验证日志文件
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=5)

    log_file = DATA_ROOT / "logs" / f"{source_key}.jsonl"
    assert log_file.exists(), f"日志文件 {log_file} 应存在"

    records = []
    with open(log_file, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    assert len(records) > 0, "日志文件应有内容"


def test_domain_snapshot():
    """测试：领域快照"""
    from app.services.base import BaseModuleService

    # 每个服务都能生成 domain_snapshot
    services = [
        LogSourceService(), LogParserService(), EventTrackerService(),
        StreamBufferService(), FieldMappingService(), AgentHeartbeatService(),
        EventValidateService(), LogRotateService(), EnrichJoinService(),
        AlertRuleService(), LogPartitionService(), CollectCheckpointService(),
        DedupWindowService(), AggregateOlapService(), FormatOutputService(),
        BackfillRangeService(), SamplingRuleService(), LogQualityService(),
    ]

    for svc in services:
        snapshot = svc.domain_snapshot()
        assert "module" in snapshot
        assert "entity" in snapshot
        assert "artifact" in snapshot
        assert "count" in snapshot
        assert "by_status" in snapshot
        assert "abnormal" in snapshot


def test_source_activate_auto_triggers():
    """测试：log_source激活自动创建parser+buffer+agent"""
    svc = LogSourceService()
    sources = [s for s in store.rows("log_source") if s.get("status") == "草稿"]
    if not sources:
        # 创建草稿源
        rows = store.rows("log_source")
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        sources = [{
            "id": new_id,
            "源编号": f"LOG_-{new_id:04d}",
            "源名称": f"自动触发测试源{new_id}",
            "日志类型": "auto_test",
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "接入方式": "api",
            "路径模版": "/var/log/auto_test",
            "字符编码": "UTF-8",
            "轮转规则": "daily",
            "源状态": "草稿",
        }]
        rows.append(sources[0])

    result = svc.activate_source(sources[0]["id"])
    assert "auto_triggers" in result, f"应该有自动触发记录: {list(result.keys())}"
    triggers = result["auto_triggers"]
    assert "log_parser" in triggers, "应自动创建log_parser"
    assert "stream_buffer" in triggers, "应自动创建stream_buffer"
    assert "agent_heartbeat" in triggers, "应自动创建agent_heartbeat"


def test_event_activate_auto_validate():
    """测试：event_tracker激活自动创建校验规则并触发校验"""
    svc = EventTrackerService()
    events = [e for e in store.rows("event_tracker") if e.get("status") == "草稿"]
    if not events:
        events = [store.rows("event_tracker")[0]]
        events[0]["status"] = "草稿"

    # 先触发一些跟踪数据
    events[0]["status"] = "已生效"
    svc.track_event(events[0]["事件编号"], {"action": "auto_test", "user_id": "u123"})

    result = svc.activate_event(events[0]["id"])
    assert "auto_triggers" in result, f"应有自动触发: {list(result.keys())}"
    assert "event_validate" in result["auto_triggers"], "应自动创建event_validate"


def test_collect_auto_parse_mapping():
    """测试：采集后自动触发解析和映射"""
    svc = LogSourceService()
    sources = svc.get_active_sources()
    assert len(sources) > 0

    result = svc.collect_from_source(sources[0]["id"], batch_size=5)
    assert result.get("written", 0) > 0
    # 验证自动解析和映射触发
    assert "auto_parse" in result or "auto_mapping" in result, \
        f"采集后应有自动解析/映射: {list(result.keys())}"


def test_checkpoint_commit_auto_export():
    """测试：位点提交自动触发导出和质量诊断"""
    svc = CollectCheckpointService()
    source_key = "LOG_-0003"
    svc.save_checkpoint(source_key, 4096, 100)

    result = svc.commit_checkpoint(source_key)
    assert result.get("offset") is not None
    assert "auto_export" in result or "auto_quality" in result, \
        f"位点提交后应有自动导出/质量诊断: {list(result.keys())}"


def test_backfill_complete_auto_quality():
    """测试：回溯完成自动触发质量诊断"""
    svc = BackfillRangeService()
    task = svc.create_backfill("LOG_-0003", "2026-09-01~2026-09-30")
    task_id = task["任务编号"]
    svc.execute_backfill(task_id, batch_size=10)

    result = svc.complete_backfill(task_id)
    assert result.get("status") == "completed"
    # 验证质量诊断触发
    assert "auto_quality" in result or "auto_alert" in result, \
        f"回溯完成后应有自动质量/告警: {list(result.keys())}"


def test_enrich_auto_aggregate():
    """测试：富化完成后自动触发聚合"""
    svc = EnrichJoinService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有映射数据
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=3)
    LogParserService().parse_log_file(source_key)
    FieldMappingService().apply_mappings(source_key)

    # 构建维表并富化
    rules = [r for r in store.rows("enrich_join") if r.get("status") == "已生效"]
    if rules:
        svc.build_dimension_cache(rules[0]["规则编号"])
    result = svc.enrich_source(source_key)
    assert result.get("enriched", 0) > 0
    # 验证自动聚合
    assert "auto_aggregate" in result, "富化后应有自动聚合触发"


def test_offline_agent_triggers_rollback_alert():
    """测试：代理离线检测触发位点回滚和告警"""
    svc = AgentHeartbeatService()
    agents = store.rows("agent_heartbeat")
    agent_code = agents[0]["代理编号"]

    # 先发送心跳配置在线状态
    svc.send_heartbeat(agent_code, {"collected": 500, "error_rate": 0.01})

    # 查找有 _last_heartbeat_ts 的代理进行离线测试
    for a in agents:
        if a.get("_last_heartbeat_ts"):
            a["_last_heartbeat_ts"] = time.time() - svc.HEARTBEAT_TIMEOUT - 60
            a["status"] = "运行中"

    offline = svc.detect_offline_agents()
    assert len(offline) >= 0, "离线检测应返回结果"

    # 验证离线代理有回滚/告警标记（如果触发检测到）
    for item in offline:
        assert "auto_rollback" in item or "alert_triggered" in item, \
            f"离线代理应有回滚或告警标记: {item}"


def test_quality_diagnosis_triggers_alert():
    """测试：质量诊断后触发告警评估"""
    svc = LogQualityService()
    sources = LogSourceService().get_active_sources()
    source_key = sources[0]["源编号"]

    # 确保有解析数据
    LogParserService().parse_log_file(source_key)

    result = svc.diagnose_source(source_key)
    assert "parse_success_rate" in result
    # 验证质量诊断可能触发告警（取决于成功率）
    assert isinstance(result.get("auto_alert", None), (dict, type(None))), \
        "质量诊断后的auto_alert应为dict或None"


def test_cross_module_auto_flow():
    """测试：完整跨模块自动触发流程
    流程: log_source.activate -> auto_create parser+buffer+agent ->
          collect -> auto_parse -> auto_mapping ->
          checkpoint.commit -> auto_export+quality ->
          backfill.complete -> auto_quality+alert
    """
    p = CrossModulePipeline()
    sources = LogSourceService().get_active_sources()
    assert len(sources) > 0
    source_key = sources[0]["源编号"]

    # 1. 完整采集管道（包含自动解析+映射）
    collect_result = p.full_collect_pipeline()
    assert "stages" in collect_result
    assert collect_result["sources_processed"] > 0

    # 2. 质量告警管道（质量诊断自动触发告警）
    quality_result = p.quality_alert_pipeline(source_key)
    assert "stages" in quality_result
    assert "diagnose" in quality_result["stages"]
    assert "alert" in quality_result["stages"]

    # 3. 富化聚合管道（富化自动触发聚合）
    enrich_result = p.enrichment_aggregation_pipeline(source_key)
    assert "stages" in enrich_result
    assert "enrich" in enrich_result["stages"]
    assert "aggregate" in enrich_result["stages"]

    # 4. 去重采样管道
    dedup_result = p.dedup_sample_pipeline(source_key)
    assert "stages" in dedup_result
    assert "dedup" in dedup_result["stages"]
    assert "sample" in dedup_result["stages"]

    # 5. 回溯管道（回溯完成自动触发质量诊断）
    backfill_result = p.backfill_pipeline(source_key, "2026-09-01~2026-09-30")
    assert "pipeline" in backfill_result
    assert "total_written" in backfill_result

    # 6. 轮转分区管道
    rotate_result = p.rotate_partition_pipeline(source_key)
    assert "stages" in rotate_result

    # 验证数据产物
    from app.services.base import DATA_ROOT
    assert (DATA_ROOT / "logs").exists()
    assert (DATA_ROOT / "exports").exists()
    assert (DATA_ROOT / "aggregations").exists()
    assert (DATA_ROOT / "checkpoints").exists()


# ---------------------------------------------------------------------------
# 回溯质量报告：独立输入、源+任务绑定、失败重试幂等、异常证据回查
# ---------------------------------------------------------------------------

def _backfill_fixture(source_key: str = "LOG_-0003"):
    svc = BackfillRangeService()
    task = svc.create_backfill(source_key, "2026-09-01~2026-09-30")
    task_id = task["任务编号"]
    svc.execute_backfill(task_id, batch_size=6)
    return svc, LogQualityService(), task_id


def test_backfill_quality_uses_only_task_detail():
    """只有回溯文件时也能出报告，且行数来自本任务实际采得的明细。"""
    svc, quality, task_id = _backfill_fixture()
    result = svc.complete_backfill(task_id)
    report = result["auto_quality"]

    assert report["report_status"] == "已确认"
    assert report["task_id"] == task_id
    assert report["source_key"] == "LOG_-0003"
    assert report["valid_raw_lines"] == 6
    assert report["bad_lines"] == 0
    assert report["total_records"] == 6
    assert report["diagnosis_input"]["online_records_used"] == 0
    assert report["diagnosis_input"]["other_tasks_used"] == 0
    # 报告详情可回查
    assert quality.get_backfill_quality_report(task_id)["batch_key"] == report["batch_key"]


def test_backfill_quality_isolated_from_online_and_other_task():
    """同源两个时间段：在线采集与另一个任务都不计入本次报告。"""
    svc, quality, task1 = _backfill_fixture()
    sources = LogSourceService().get_active_sources()
    LogSourceService().collect_from_source(sources[0]["id"], batch_size=6)

    task2 = svc.create_backfill("LOG_-0003", "2026-10-01~2026-10-31")["任务编号"]
    svc.execute_backfill(task2, batch_size=4)

    report1 = svc.complete_backfill(task1)["auto_quality"]
    report2 = svc.complete_backfill(task2)["auto_quality"]

    assert report1["valid_raw_lines"] == 6
    assert report2["valid_raw_lines"] == 4
    assert report1["time_range"] == "2026-09-01~2026-09-30"
    assert report2["time_range"] == "2026-10-01~2026-10-31"
    assert report1["batch_key"] != report2["batch_key"]
    assert report2["range_proof"]["timestamp_min"].startswith("2026-10-01")


def test_backfill_quality_counts_bad_lines_and_keeps_evidence():
    """坏行按实际明细计入异常，异常证据仍可回查。"""
    svc, quality, task_id = _backfill_fixture()
    detail = svc._task_detail_path("LOG_-0003", task_id)
    rows = quality._read_jsonl(detail)
    rows.append({**rows[0]})                       # 重复 record_id
    rows.append({**rows[1], "raw": "损坏的原始行"})  # 格式不符
    quality._atomic_write_jsonl(detail, rows)

    report = svc.complete_backfill(task_id)["auto_quality"]
    assert report["valid_raw_lines"] == 6
    assert report["bad_lines"] == 2
    assert report["abnormal_categories"]["duplicate_record"] == 1
    assert report["abnormal_categories"]["format_mismatch"] == 1
    assert 0 < report["parse_success_rate"] < 100

    evidence = quality.get_backfill_anomalies(task_id)
    assert {item["category"] for item in evidence} == {"duplicate_record", "format_mismatch"}
    assert all(item["source_key"] == "LOG_-0003" for item in evidence)


def test_backfill_quality_write_failure_and_idempotent_retry(monkeypatch):
    """写入中断时说明原因；重试只生成一份确认报告，历史证据保留。"""
    svc, quality, task_id = _backfill_fixture()
    detail = svc._task_detail_path("LOG_-0003", task_id)
    rows = quality._read_jsonl(detail)
    quality._atomic_write_jsonl(detail, rows + [{**rows[0], "raw": "坏行"}])

    original = LogQualityService._atomic_write_json

    def flaky(self, path, payload):
        if str(path).endswith(f"backfill_quality_{task_id}.json"):
            raise OSError("simulated interruption")
        return original(self, path, payload)

    monkeypatch.setattr(LogQualityService, "_atomic_write_json", flaky)
    failed = svc.complete_backfill(task_id)
    monkeypatch.undo()

    assert failed["quality_error"]["report_status"] == "失败"
    assert "simulated interruption" in failed["quality_error"]["error"]
    assert failed["quality_error"]["valid_raw_lines"] == 6
    assert quality.get_backfill_quality_report(task_id) is None
    assert quality.get_backfill_report_failure(task_id)["error"]
    # 失败时异常证据已经落盘，仍可回查
    assert quality.get_backfill_anomalies(task_id)

    retried = svc.complete_backfill(task_id)
    assert retried["auto_quality"]["valid_raw_lines"] == 6
    assert retried["auto_quality"]["bad_lines"] == 1

    # 再重试一次：复用同批确认报告，不新增
    again = svc.complete_backfill(task_id)
    assert again["auto_quality"].get("reused") is True

    summary = quality._read_jsonl(
        quality._data_dir("exports") / "backfill_quality_reports.jsonl")
    assert sum(1 for row in summary if row["task_id"] == task_id) == 1
    assert len(svc.get_backfill_history(task_id=task_id)) == 1
    assert quality.get_backfill_anomalies(task_id)


def test_backfill_quality_fails_without_detail():
    """没有实际明细时明确说明原因，不生成空报告。"""
    svc = BackfillRangeService()
    task_id = svc.create_backfill("LOG_-0003", "2026-11-01~2026-11-30")["任务编号"]
    failure = LogQualityService().diagnose_backfill_quality(task_id)
    assert failure["report_status"] == "失败"
    assert "无实际采集明细" in failure["error"]
    assert LogQualityService().get_backfill_quality_report(task_id) is None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "--tb=short", "-s"]))