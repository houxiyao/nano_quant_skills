"""ClickHouse data quality checker."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger

from tushare_to_clickhouse.clickhouse_client import ClickHouseManager


class QualityChecker:
    """Runs quality checks on ClickHouse tables."""

    def __init__(self, ch: ClickHouseManager):
        self.ch = ch

    def check_table(
        self,
        table: str,
        pk_cols: List[str],
        date_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run quality checks on a ClickHouse table."""
        client = self.ch.client
        report: Dict[str, Any] = {
            "table": table,
            "check_time": datetime.now().isoformat(),
            "checks": {},
            "passed": True,
        }

        # 1. Row count
        result = client.query(f'SELECT COUNT() FROM "{table}"')
        row_count = int(result.result_rows[0][0]) if result.result_rows else 0
        report["checks"]["row_count"] = {"value": row_count, "pass": row_count > 0}

        # 2. PK uniqueness
        pk_expr = ", ".join(f'"{c}"' for c in pk_cols)
        result = client.query(
            f"SELECT {pk_expr}, COUNT() AS cnt FROM \"{table}\" GROUP BY {pk_expr} HAVING cnt > 1 LIMIT 5"
        )
        dup_count = len(result.result_rows)
        report["checks"]["pk_unique"] = {
            "value": dup_count,
            "pass": dup_count == 0,
            "sample": [str(r) for r in result.result_rows] if result.result_rows else [],
        }

        # 3. PK null check
        pk_nulls = {}
        for col in pk_cols:
            result = client.query(f'SELECT COUNT() FROM "{table}" WHERE "{col}" IS NULL')
            pk_nulls[col] = int(result.result_rows[0][0]) if result.result_rows else 0
        all_pk_clean = all(v == 0 for v in pk_nulls.values())
        report["checks"]["pk_no_null"] = {"value": pk_nulls, "pass": all_pk_clean}

        # 4. Date range
        if date_col:
            result = client.query(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}"')
            if result.result_rows:
                report["checks"]["date_range"] = {
                    "min": str(result.result_rows[0][0]),
                    "max": str(result.result_rows[0][1]),
                    "pass": True,
                }

        # 5. Distinct ts_code
        db = client.database or "default"
        result = client.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tbl:String} AND name = 'ts_code'",
            parameters={"db": db, "tbl": table},
        )
        if result.result_rows:
            result = client.query(f'SELECT COUNT(DISTINCT "ts_code") FROM "{table}"')
            report["checks"]["distinct_ts_code"] = {
                "value": int(result.result_rows[0][0]) if result.result_rows else 0
            }

        # 6. Distinct dates
        if date_col:
            result = client.query(f'SELECT COUNT(DISTINCT "{date_col}") FROM "{table}"')
            report["checks"]["distinct_dates"] = {
                "value": int(result.result_rows[0][0]) if result.result_rows else 0
            }

        # 7. NaN string pollution
        result = client.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tbl:String} AND type = 'String'",
            parameters={"db": db, "tbl": table},
        )
        nan_cols = []
        for row in result.result_rows:
            col = row[0]
            r = client.query(
                f'''SELECT COUNT() FROM "{table}" WHERE "{col}" IN ('nan', 'NaN', 'NAN', 'None')'''
            )
            cnt = int(r.result_rows[0][0]) if r.result_rows else 0
            if cnt > 0:
                nan_cols.append({"column": col, "count": cnt})
        report["checks"]["nan_pollution"] = {
            "value": nan_cols,
            "pass": len(nan_cols) == 0,
        }

        # 8. High null measure columns
        result = client.query(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {tbl:String} "
            "AND type IN ('Float64', 'Float32', 'Int64', 'Int32')",
            parameters={"db": db, "tbl": table},
        )
        high_null_cols = []
        if row_count > 0:
            for row in result.result_rows:
                col = row[0]
                r = client.query(f'SELECT COUNT() FROM "{table}" WHERE "{col}" IS NULL')
                null_cnt = int(r.result_rows[0][0]) if r.result_rows else 0
                ratio = null_cnt / row_count
                if ratio > 0.5:
                    high_null_cols.append({"column": col, "null_ratio": round(ratio, 4)})
        report["checks"]["high_null_measures"] = {
            "value": high_null_cols,
            "pass": len(high_null_cols) == 0,
        }

        # Overall pass
        for check in report["checks"].values():
            if "pass" in check and not check["pass"]:
                report["passed"] = False
                break

        return report

    @staticmethod
    def format_markdown(report: Dict[str, Any]) -> str:
        checks = report["checks"]
        t = report["check_time"]
        lines = [
            "| 指标 | 值 | 检查时间 |",
            "|---|---|---|",
            f"| 总行数 | {checks['row_count']['value']} | {t} |",
        ]
        if "date_range" in checks:
            lines.append(f"| 数据起始 | {checks['date_range']['min']} | {t} |")
            lines.append(f"| 数据截止 | {checks['date_range']['max']} | {t} |")
        if "distinct_ts_code" in checks:
            lines.append(f"| 股票数 (DISTINCT ts_code) | {checks['distinct_ts_code']['value']} | {t} |")
        if "distinct_dates" in checks:
            lines.append(f"| 交易日/报告期数 | {checks['distinct_dates']['value']} | {t} |")
        lines.append(f"| PK 重复数 | {checks['pk_unique']['value']} | {t} |")
        pk_null_str = ", ".join(f"{k}={v}" for k, v in checks["pk_no_null"]["value"].items())
        lines.append(f"| PK 列 NULL 数 | {pk_null_str} | {t} |")
        nan_val = checks["nan_pollution"]["value"]
        nan_str = "无" if not nan_val else ", ".join(f"{x['column']}({x['count']})" for x in nan_val)
        lines.append(f"| NaN 字符串污染列 | {nan_str} | {t} |")
        high_null = checks["high_null_measures"]["value"]
        hn_str = "无" if not high_null else ", ".join(f"{x['column']}({x['null_ratio']:.1%})" for x in high_null)
        lines.append(f"| 度量列全 NULL 率 > 50% | {hn_str} | {t} |")
        return "\n".join(lines)
