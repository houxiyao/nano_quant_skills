"""一键任务入口：通过环境变量或配置文件驱动 Tushare -> DuckDB 同步。

示例：
python etl/run_tushare_to_duckdb_task.py \
  --config-file etl/tushare_to_duckdb.properties.example
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

import duckdb
from loguru import logger

try:
    from tushare_duckdb_sync_scripts.etl.tushare_to_duckdb import StructuredETLError, run_etl
except ModuleNotFoundError:
    try:
        from etl.tushare_to_duckdb import StructuredETLError, run_etl
    except ModuleNotFoundError:
        project_root = Path(__file__).resolve().parents[1]
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from etl.tushare_to_duckdb import StructuredETLError, run_etl


def _log_event(event: str, payload: Dict[str, object]) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=True))


def _parse_properties_file(config_path: Path) -> Dict[str, str]:
    items: Dict[str, str] = {}
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            continue
        items[key.strip()] = value.strip()
    return items


def _load_config_file(config_file: Optional[str]) -> Dict[str, str]:
    if not config_file:
        return {}

    path = Path(config_file).expanduser().resolve()
    if not path.exists():
        raise StructuredETLError("Config file not found", {"config_file": str(path)})

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise StructuredETLError("JSON config must be an object", {"config_file": str(path)})
        return {str(k): "" if v is None else str(v) for k, v in data.items()}
    return _parse_properties_file(path)


def _pick(config: Dict[str, str], env_name: str, default: Optional[str] = None) -> Optional[str]:
    env_value = os.environ.get(env_name)
    if env_value is not None and env_value != "":
        return env_value
    conf_value = config.get(env_name)
    if conf_value is not None and conf_value != "":
        return conf_value
    return default


def _pick_int(config: Dict[str, str], env_name: str, default: Optional[int] = None) -> Optional[int]:
    value = _pick(config, env_name, None)
    if value is None:
        return default
    return int(value)


def _pick_float(config: Dict[str, str], env_name: str, default: Optional[float] = None) -> Optional[float]:
    value = _pick(config, env_name, None)
    if value is None:
        return default
    return float(value)


def _pick_bool(config: Dict[str, str], env_name: str, default: bool = False) -> bool:
    value = _pick(config, env_name, None)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _build_base_args(config: Dict[str, str]) -> argparse.Namespace:

    endpoint = _pick(config, "TS_DUCKDB_ENDPOINT")
    duckdb_path = _pick(config, "TS_DUCKDB_DUCKDB_PATH")

    if not endpoint:
        raise StructuredETLError(
            "TS_DUCKDB_ENDPOINT is required",
            {"missing_env": "TS_DUCKDB_ENDPOINT"},
        )
    if not duckdb_path:
        raise StructuredETLError(
            "TS_DUCKDB_DUCKDB_PATH is required",
            {"missing_env": "TS_DUCKDB_DUCKDB_PATH"},
        )

    return argparse.Namespace(
        endpoint=endpoint,
        method=_pick(config, "TS_DUCKDB_METHOD", "query"),
        source_table=_pick(config, "TS_DUCKDB_SOURCE_TABLE", endpoint),
        duckdb_path=duckdb_path,
        target_table=_pick(config, "TS_DUCKDB_TARGET_TABLE"),
        mode=_pick(config, "TS_DUCKDB_MODE", "overwrite"),
        dimension_type=_pick(config, "TS_DUCKDB_DIMENSION_TYPE", "none"),
        dimension_field=_pick(config, "TS_DUCKDB_DIMENSION_FIELD"),
        start_date=_pick(config, "TS_DUCKDB_START_DATE"),
        end_date=_pick(config, "TS_DUCKDB_END_DATE"),
        sync_all=_pick_bool(config, "TS_DUCKDB_SYNC_ALL", False),
        params=_pick(config, "TS_DUCKDB_PARAMS"),
        limit=_pick_int(config, "TS_DUCKDB_LIMIT"),
        max_retries=_pick_int(config, "TS_DUCKDB_MAX_RETRIES", 3),
        base_sleep_seconds=_pick_float(config, "TS_DUCKDB_BASE_SLEEP_SECONDS", 2.0),
        sleep_seconds=_pick_float(config, "TS_DUCKDB_SLEEP_SECONDS", 0.0),
        allow_empty_result=_pick_bool(config, "TS_DUCKDB_ALLOW_EMPTY_RESULT", False),
    )


def _bool_from_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_from_value(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    return int(value)


def _float_from_value(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    return float(value)


def _pick_from_task(task: Dict[str, Any], key: str, default: Any = None) -> Any:
    value = task.get(key)
    if value is None or value == "":
        return default
    return value


def _parse_params_object(value: Any) -> Dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise StructuredETLError("Task params must be a JSON object", {"params": value})
        return parsed
    raise StructuredETLError("Task params must be a JSON object", {"params_type": type(value).__name__})


def _serialize_params_object(value: Any) -> Optional[str]:
    params = _parse_params_object(value)
    if not params:
        return None
    return json.dumps(params, ensure_ascii=False)


def _resolve_task_param_overlays(task: Dict[str, Any], duckdb_path: Optional[str]) -> List[Dict[str, Any]]:
    param_sets = task.get("param_sets")
    params_sql = task.get("params_sql")

    if param_sets is not None and params_sql is not None:
        raise StructuredETLError(
            "Task cannot define both param_sets and params_sql",
            {"task": task},
        )

    if param_sets is None and params_sql is None:
        return [{}]

    if param_sets is not None:
        if not isinstance(param_sets, list):
            raise StructuredETLError("task.param_sets must be a JSON array", {"task": task})
        overlays: List[Dict[str, Any]] = []
        for index, item in enumerate(param_sets):
            if not isinstance(item, dict):
                raise StructuredETLError(
                    "Each item in task.param_sets must be a JSON object",
                    {"task": task, "param_set_index": index},
                )
            overlays.append(dict(item))
        if not overlays:
            raise StructuredETLError("task.param_sets cannot be empty", {"task": task})
        return overlays

    if not duckdb_path:
        raise StructuredETLError(
            "duckdb_path is required when task uses params_sql",
            {"task": task},
        )

    duckdb_file = Path(duckdb_path).expanduser().resolve()
    if not duckdb_file.exists():
        raise StructuredETLError(
            "DuckDB file not found for params_sql expansion",
            {"duckdb_path": str(duckdb_file), "task": task},
        )

    with duckdb.connect(str(duckdb_file), read_only=True) as con:
        cursor = con.execute(str(params_sql))
        columns = [str(column[0]) for column in (cursor.description or [])]
        rows = cursor.fetchall()

    overlays = [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
    if not overlays:
        raise StructuredETLError(
            "task.params_sql returned no rows",
            {"duckdb_path": str(duckdb_file), "params_sql": params_sql},
        )
    return overlays


def _build_task_args(base_args: argparse.Namespace, task: Dict[str, Any]) -> argparse.Namespace:
    endpoint = _pick_from_task(task, "endpoint", base_args.endpoint)
    if not endpoint:
        raise StructuredETLError(
            "Task endpoint is required",
            {"task": task},
        )

    duckdb_path = _pick_from_task(task, "duckdb_path", base_args.duckdb_path)
    if not duckdb_path:
        raise StructuredETLError(
            "Task duckdb_path is required",
            {"task": task},
        )

    source_table = _pick_from_task(task, "source_table", base_args.source_table)
    source_table = source_table or endpoint

    return argparse.Namespace(
        endpoint=endpoint,
        method=_pick_from_task(task, "method", base_args.method),
        source_table=source_table,
        duckdb_path=duckdb_path,
        target_table=_pick_from_task(task, "target_table", base_args.target_table),
        mode=_pick_from_task(task, "mode", base_args.mode),
        dimension_type=_pick_from_task(task, "dimension_type", base_args.dimension_type),
        dimension_field=_pick_from_task(task, "dimension_field", base_args.dimension_field),
        start_date=_pick_from_task(task, "start_date", base_args.start_date),
        end_date=_pick_from_task(task, "end_date", base_args.end_date),
        sync_all=_bool_from_value(task.get("sync_all"), bool(base_args.sync_all)),
        params=_serialize_params_object(_pick_from_task(task, "params", base_args.params)),
        limit=_int_from_value(task.get("limit"), base_args.limit),
        max_retries=_int_from_value(task.get("max_retries"), base_args.max_retries),
        base_sleep_seconds=_float_from_value(task.get("base_sleep_seconds"), base_args.base_sleep_seconds),
        sleep_seconds=_float_from_value(task.get("sleep_seconds"), base_args.sleep_seconds),
        allow_empty_result=_bool_from_value(task.get("allow_empty_result"), bool(base_args.allow_empty_result)),
    )


def _expand_task_definition(base_args: argparse.Namespace, task: Dict[str, Any]) -> List[argparse.Namespace]:
    duckdb_path = _pick_from_task(task, "duckdb_path", base_args.duckdb_path)
    overlays = _resolve_task_param_overlays(task, duckdb_path)
    base_params = _parse_params_object(_pick_from_task(task, "params", base_args.params))
    base_mode = _pick_from_task(task, "mode", base_args.mode)

    expanded_args: List[argparse.Namespace] = []
    for index, overlay in enumerate(overlays):
        expanded_task = dict(task)
        merged_params = dict(base_params)
        merged_params.update(overlay)
        expanded_task["params"] = merged_params if merged_params else None
        if index > 0 and base_mode == "overwrite":
            expanded_task["mode"] = "append"
        expanded_args.append(_build_task_args(base_args, expanded_task))
    return expanded_args


def _load_tasks_from_file(tasks_file: str) -> List[Dict[str, Any]]:
    path = Path(tasks_file).expanduser().resolve()
    if not path.exists():
        raise StructuredETLError("Tasks file not found", {"tasks_file": str(path)})

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise StructuredETLError("Tasks file must be a JSON array", {"tasks_file": str(path)})

    tasks: List[Dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise StructuredETLError(
                "Task item must be a JSON object",
                {"tasks_file": str(path), "task_index": i},
            )
        tasks.append(item)
    return tasks


def _load_tasks(config: Dict[str, str], cli_tasks_file: Optional[str]) -> List[Dict[str, Any]]:
    tasks_file = cli_tasks_file or _pick(config, "TS_DUCKDB_TASKS_FILE")
    if tasks_file:
        return _load_tasks_from_file(tasks_file)

    tasks_json = _pick(config, "TS_DUCKDB_TASKS_JSON")
    if not tasks_json:
        return []

    raw = json.loads(tasks_json)
    if not isinstance(raw, list):
        raise StructuredETLError("TS_DUCKDB_TASKS_JSON must be a JSON array", {"tasks_json": tasks_json})

    tasks: List[Dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise StructuredETLError("Task item must be a JSON object", {"task_index": i})
        tasks.append(item)
    return tasks


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Tushare to DuckDB task with config support")
    parser.add_argument(
        "--config-file",
        default=None,
        help="Optional config file (.properties/.json). Env vars override same keys",
    )
    parser.add_argument(
        "--tasks-file",
        default=None,
        help="Optional JSON file containing an array of per-table task objects",
    )
    cli_args = parser.parse_args()

    try:
        config = _load_config_file(cli_args.config_file)
        base_args = _build_base_args(config)
        continue_on_error = _pick_bool(config, "TS_DUCKDB_CONTINUE_ON_ERROR", False)
        task_definitions = _load_tasks(config, cli_args.tasks_file)

        _log_event(
            "task_started",
            {
                "task_count": len(task_definitions) if task_definitions else 1,
                "default_duckdb_path": base_args.duckdb_path,
                "continue_on_error": continue_on_error,
                "config_file": cli_args.config_file or "",
                "tasks_file": cli_args.tasks_file or _pick(config, "TS_DUCKDB_TASKS_FILE", "") or "",
            },
        )

        summary: List[Dict[str, object]] = []
        failed: List[Dict[str, object]] = []
        concrete_task_index = 0
        if task_definitions:
            for definition_index, task_definition in enumerate(task_definitions, start=1):
                expanded_tasks = _expand_task_definition(base_args, task_definition)
                _log_event(
                    "table_task_expanded",
                    {
                        "definition_index": definition_index,
                        "expanded_count": len(expanded_tasks),
                        "endpoint": _pick_from_task(task_definition, "endpoint", base_args.endpoint),
                        "source_table": _pick_from_task(task_definition, "source_table", base_args.source_table),
                        "target_table": _pick_from_task(task_definition, "target_table", base_args.target_table),
                    },
                )

                for one in expanded_tasks:
                    concrete_task_index += 1
                    _log_event(
                        "table_task_started",
                        {
                            "task_index": concrete_task_index,
                            "task_count": len(task_definitions),
                            "endpoint": one.endpoint,
                            "source_table": one.source_table,
                            "target_table": one.target_table or one.source_table,
                            "dimension_type": one.dimension_type,
                            "sync_all": bool(one.sync_all),
                        },
                    )
                    try:
                        result = run_etl(one)
                        result["task_index"] = concrete_task_index
                        summary.append(result)
                        _log_event("table_task_completed", result)
                    except Exception as exc:
                        error_payload = {
                            "task_index": concrete_task_index,
                            "endpoint": one.endpoint,
                            "source_table": one.source_table,
                            "target_table": one.target_table or one.source_table,
                            "error": str(exc),
                        }
                        failed.append(error_payload)
                        _log_event("table_task_failed", error_payload)
                        if not continue_on_error:
                            raise
        else:
            concrete_task_index = 1
            _log_event(
                "table_task_started",
                {
                    "task_index": concrete_task_index,
                    "task_count": 1,
                    "endpoint": base_args.endpoint,
                    "source_table": base_args.source_table,
                    "target_table": base_args.target_table or base_args.source_table,
                    "dimension_type": base_args.dimension_type,
                    "sync_all": bool(base_args.sync_all),
                },
            )
            try:
                result = run_etl(base_args)
                result["task_index"] = concrete_task_index
                summary.append(result)
                _log_event("table_task_completed", result)
            except Exception as exc:
                error_payload = {
                    "task_index": concrete_task_index,
                    "endpoint": base_args.endpoint,
                    "source_table": base_args.source_table,
                    "target_table": base_args.target_table or base_args.source_table,
                    "error": str(exc),
                }
                failed.append(error_payload)
                _log_event("table_task_failed", error_payload)
                if not continue_on_error:
                    raise

        _log_event(
            "task_completed",
            {
                "task_count": concrete_task_index,
                "success_count": len(summary),
                "failed_count": len(failed),
                "failed": failed,
            },
        )

        if failed:
            raise StructuredETLError(
                "Some table tasks failed",
                {
                    "failed_count": len(failed),
                    "failed": failed,
                },
            )
    except StructuredETLError as exc:
        _log_event("task_failed", exc.to_dict())
        raise
    except Exception as exc:
        _log_event("task_failed", {"error": str(exc)})
        raise


if __name__ == "__main__":
    main()
