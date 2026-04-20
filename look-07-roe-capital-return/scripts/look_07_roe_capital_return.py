from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

try:
    from .common import CompanyProfile, detect_company_profile, default_db_path, parse_date, connect_read_only
except ImportError:
    from common import CompanyProfile, detect_company_profile, default_db_path, parse_date, connect_read_only


REPORT_TYPE = "1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    return float(value)


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _object_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _fetch_dupont_inputs(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    """Fetch balance sheet + income data for DuPont decomposition.

    We fetch lookback_years + 1 extra year to compute averages for the oldest year.
    """
    query = f"""
    WITH params AS (
        SELECT
            CAST(? AS VARCHAR)  AS ts_code,
            CAST(? AS DATE)     AS as_of_date,
            CAST(? AS INTEGER)  AS lookback_years
    ),
    balance_yearly AS (
        SELECT
            b.ts_code,
            b.end_date,
            COALESCE(b.f_ann_date, b.ann_date, b.end_date) AS visible_date,
            b.total_assets,
            b.total_hldr_eqy_exc_min_int
        FROM fin_balance b
        CROSS JOIN params p
        WHERE b.ts_code = p.ts_code
          AND b.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM b.end_date) = 12
          AND EXTRACT(DAY FROM b.end_date) = 31
          AND COALESCE(b.f_ann_date, b.ann_date, b.end_date) <= p.as_of_date
    ),
    income_yearly AS (
        SELECT
            i.ts_code,
            i.end_date,
            COALESCE(i.f_ann_date, i.ann_date, i.end_date) AS visible_date,
            i.revenue,
            i.n_income_attr_p
        FROM fin_income i
        CROSS JOIN params p
        WHERE i.ts_code = p.ts_code
          AND i.report_type = '{REPORT_TYPE}'
          AND EXTRACT(MONTH FROM i.end_date) = 12
          AND EXTRACT(DAY FROM i.end_date) = 31
          AND COALESCE(i.f_ann_date, i.ann_date, i.end_date) <= p.as_of_date
    ),
    bal_dedup AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ts_code, end_date ORDER BY visible_date DESC
        ) AS rn FROM balance_yearly
    ),
    inc_dedup AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ts_code, end_date ORDER BY visible_date DESC
        ) AS rn FROM income_yearly
    ),
    combined AS (
        SELECT
            b.ts_code,
            b.end_date,
            b.total_assets,
            b.total_hldr_eqy_exc_min_int,
            i.revenue,
            i.n_income_attr_p
        FROM bal_dedup b
        LEFT JOIN inc_dedup i ON b.ts_code = i.ts_code AND b.end_date = i.end_date AND i.rn = 1
        WHERE b.rn = 1
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ts_code ORDER BY end_date DESC
        ) AS rn
        FROM combined
    )
    SELECT
        ts_code, end_date,
        total_assets, total_hldr_eqy_exc_min_int,
        revenue, n_income_attr_p
    FROM ranked
    WHERE rn <= (SELECT lookback_years + 2 FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


def _fetch_indicator_rows(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
    lookback_years: int,
) -> list[dict[str, Any]]:
    """Fetch ROE/ROA/turnover indicators from fin_indicator."""
    query = f"""
    WITH params AS (
        SELECT
            CAST(? AS VARCHAR)  AS ts_code,
            CAST(? AS DATE)     AS as_of_date,
            CAST(? AS INTEGER)  AS lookback_years
    ),
    indicator_yearly AS (
        SELECT
            fi.ts_code,
            fi.end_date,
            COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) AS sort_key,
            fi.roe,
            fi.roe_dt,
            fi.roa,
            fi.netprofit_margin,
            fi.assets_turn,
            fi.debt_to_assets
        FROM fin_indicator fi
        CROSS JOIN params p
        WHERE fi.ts_code = p.ts_code
          AND EXTRACT(MONTH FROM fi.end_date) = 12
          AND EXTRACT(DAY FROM fi.end_date) = 31
          AND COALESCE(fi.ann_date_key, fi.ann_date, fi.end_date) <= p.as_of_date
    ),
    deduped AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ts_code, end_date ORDER BY sort_key DESC
        ) AS rn
        FROM indicator_yearly
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ts_code ORDER BY end_date DESC
        ) AS rn2
        FROM deduped WHERE rn = 1
    )
    SELECT ts_code, end_date, roe, roe_dt, roa, netprofit_margin, assets_turn, debt_to_assets
    FROM ranked
    WHERE rn2 <= (SELECT lookback_years + 1 FROM params)
    ORDER BY end_date DESC
    """
    result = con.execute(query, [stock, as_of_date, lookback_years])
    columns = [item[0] for item in result.description]
    return [{col: val for col, val in zip(columns, record)} for record in result.fetchall()]


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

def _find_benchmark(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
) -> dict[str, Any] | None:
    if not _object_exists(con, "idx_sw_l3_peers") or not _object_exists(con, "stk_factor_pro"):
        return None

    peers = con.execute(
        """
        SELECT DISTINCT peer_ts_code, peer_name
        FROM idx_sw_l3_peers
        WHERE anchor_ts_code = ?
          AND peer_is_self = false
        """,
        [stock],
    ).fetchall()

    if not peers:
        return None

    peer_codes = [p[0] for p in peers]
    peer_name_map = {p[0]: p[1] for p in peers}

    latest_date_row = con.execute(
        """
        SELECT MAX(trade_date)
        FROM stk_factor_pro
        WHERE trade_date <= CAST(? AS DATE)
          AND total_mv IS NOT NULL
        """,
        [as_of_date],
    ).fetchone()

    if not latest_date_row or latest_date_row[0] is None:
        return None

    latest_trade_date = latest_date_row[0]
    placeholders = ", ".join(["?"] * len(peer_codes))
    top_row = con.execute(
        f"""
        SELECT ts_code, total_mv
        FROM stk_factor_pro
        WHERE trade_date = ?
          AND ts_code IN ({placeholders})
          AND total_mv IS NOT NULL
        ORDER BY total_mv DESC
        LIMIT 1
        """,
        [latest_trade_date] + peer_codes,
    ).fetchone()

    if not top_row:
        return None

    return {
        "ts_code": top_row[0],
        "name": peer_name_map.get(top_row[0], ""),
        "total_mv": float(top_row[1]),
        "mv_trade_date": (
            latest_trade_date.isoformat()
            if isinstance(latest_trade_date, date)
            else str(latest_trade_date)
        ),
    }


def _fetch_peer_industry_info(
    con: duckdb.DuckDBPyConnection,
    stock: str,
) -> dict[str, Any]:
    if not _object_exists(con, "idx_sw_l3_peers"):
        return {}
    row = con.execute(
        """
        SELECT DISTINCT l1_name, l2_name, l3_name, l3_code, peer_group_size
        FROM idx_sw_l3_peers
        WHERE anchor_ts_code = ?
        LIMIT 1
        """,
        [stock],
    ).fetchone()
    if not row:
        return {}
    return {
        "l1_name": row[0],
        "l2_name": row[1],
        "l3_name": row[2],
        "l3_code": row[3],
        "peer_group_size": row[4],
    }


# ---------------------------------------------------------------------------
# DuPont computation
# ---------------------------------------------------------------------------

def _compute_dupont(raw_rows: list[dict[str, Any]], lookback_years: int) -> list[dict[str, Any]]:
    """Compute DuPont three-factor decomposition using average balance sheet items.

    raw_rows must be sorted by end_date DESC and include 1 extra prior year for averaging.
    """
    if len(raw_rows) < 2:
        if len(raw_rows) == 1:
            row = raw_rows[0]
            return [{
                "end_date": row["end_date"],
                "revenue": _float_or_none(row.get("revenue")),
                "n_income_attr_p": _float_or_none(row.get("n_income_attr_p")),
                "total_assets": _float_or_none(row.get("total_assets")),
                "parent_equity": _float_or_none(row.get("total_hldr_eqy_exc_min_int")),
                "avg_total_assets": None,
                "avg_parent_equity": None,
                "npm": _safe_div(
                    _float_or_none(row.get("n_income_attr_p")),
                    _float_or_none(row.get("revenue")),
                ),
                "asset_turnover": None,
                "equity_multiplier": None,
                "roe_dupont": None,
            }]
        return []

    # Build a lookup by end_date for prior-year averaging
    by_date = {}
    for row in raw_rows:
        key = row["end_date"]
        by_date[key] = row

    # Sort dates descending
    sorted_dates = sorted(by_date.keys(), reverse=True)

    results = []
    # Only output lookback_years + 1 rows (the extra row is just for averaging)
    output_count = min(lookback_years + 1, len(sorted_dates) - 1)

    for i in range(output_count):
        curr_date = sorted_dates[i]
        prior_date = sorted_dates[i + 1] if (i + 1) < len(sorted_dates) else None

        curr = by_date[curr_date]
        prior = by_date.get(prior_date) if prior_date else None

        rev = _float_or_none(curr.get("revenue"))
        ni = _float_or_none(curr.get("n_income_attr_p"))
        ta_end = _float_or_none(curr.get("total_assets"))
        eq_end = _float_or_none(curr.get("total_hldr_eqy_exc_min_int"))

        ta_beg = _float_or_none(prior.get("total_assets")) if prior else None
        eq_beg = _float_or_none(prior.get("total_hldr_eqy_exc_min_int")) if prior else None

        # Averages
        avg_ta = None
        if ta_end is not None and ta_beg is not None:
            avg_ta = (ta_beg + ta_end) / 2
        avg_eq = None
        if eq_end is not None and eq_beg is not None:
            avg_eq = (eq_beg + eq_end) / 2

        npm = _safe_div(ni, rev)
        at = _safe_div(rev, avg_ta)
        # Negative equity makes EM meaningless; flag it explicitly
        negative_equity = avg_eq is not None and avg_eq <= 0
        em = _safe_div(avg_ta, avg_eq) if not negative_equity else None

        # DuPont ROE = npm * at * em
        roe_dupont = None
        if npm is not None and at is not None and em is not None:
            roe_dupont = npm * at * em

        results.append({
            "end_date": curr_date,
            "revenue": rev,
            "n_income_attr_p": ni,
            "total_assets": ta_end,
            "parent_equity": eq_end,
            "avg_total_assets": avg_ta,
            "avg_parent_equity": avg_eq,
            "negative_equity": negative_equity,
            "npm": npm,
            "asset_turnover": at,
            "equity_multiplier": em,
            "roe_dupont": roe_dupont,
        })

    return results


# ---------------------------------------------------------------------------
# ROE driver classification
# ---------------------------------------------------------------------------

def _classify_roe_driver(dupont_rows: list[dict[str, Any]]) -> str:
    """Classify ROE driver type based on latest DuPont factors."""
    if not dupont_rows:
        return "insufficient-data"

    latest = dupont_rows[0]
    npm = latest.get("npm")
    at = latest.get("asset_turnover")
    em = latest.get("equity_multiplier")
    roe = latest.get("roe_dupont")

    # Negative equity (资不抵债) is worse than just negative ROE
    if latest.get("negative_equity"):
        return "negative-equity"
    if roe is None:
        return "insufficient-data"
    if roe < 0:
        return "negative-roe"

    if npm is None or at is None or em is None:
        return "insufficient-data"

    # Heuristic classification:
    # High leverage: EM > 5 and NPM < 0.08
    # Profitability-driven: NPM > 0.10 and EM < 4
    # Turnover-driven: AT > 1.0 and NPM < 0.08
    # Otherwise: mixed
    if em > 5 and npm < 0.08:
        return "leverage-driven"
    if npm > 0.10 and em < 4:
        return "profitability-driven"
    if at > 1.0 and npm < 0.08:
        return "turnover-driven"
    return "mixed"


def _assess_roe_trend(dupont_rows: list[dict[str, Any]]) -> str:
    """Assess ROE trend over lookback period."""
    roe_values = [
        r["roe_dupont"] for r in dupont_rows if r.get("roe_dupont") is not None
    ]
    if len(roe_values) < 2:
        return "insufficient-data"

    newest = roe_values[0]
    oldest = roe_values[-1]

    if oldest == 0:
        return "volatile" if newest != 0 else "stable"

    change_ratio = (newest - oldest) / abs(oldest)
    if change_ratio > 0.15:
        return "improving"
    if change_ratio < -0.15:
        return "deteriorating"
    return "stable"


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _build_summary(
    dupont_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not dupont_rows:
        return {
            "years_returned": 0,
            "roe_latest": None,
            "roe_driver": "insufficient-data",
            "roe_trend": "insufficient-data",
            "npm_latest": None,
            "at_latest": None,
            "em_latest": None,
        }

    latest = dupont_rows[0]
    return {
        "years_returned": len(dupont_rows),
        "latest_end_date": (
            latest["end_date"].isoformat()
            if isinstance(latest["end_date"], date)
            else str(latest["end_date"])
        ),
        "roe_latest": latest.get("roe_dupont"),
        "roe_driver": _classify_roe_driver(dupont_rows),
        "roe_trend": _assess_roe_trend(dupont_rows),
        "npm_latest": latest.get("npm"),
        "at_latest": latest.get("asset_turnover"),
        "em_latest": latest.get("equity_multiplier"),
    }


def _build_comparison(
    target: list[dict[str, Any]],
    bench: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    bench_by_date = {}
    for r in bench:
        key = r["end_date"].isoformat() if isinstance(r["end_date"], date) else str(r["end_date"])
        bench_by_date[key] = r

    comparison = []
    for t in target:
        key = t["end_date"].isoformat() if isinstance(t["end_date"], date) else str(t["end_date"])
        b = bench_by_date.get(key, {})
        entry = {"end_date": key}
        for metric in ("roe_dupont", "npm", "asset_turnover", "equity_multiplier"):
            tv = t.get(metric)
            bv = b.get(metric)
            entry[f"target_{metric}"] = tv
            entry[f"bench_{metric}"] = bv
        comparison.append(entry)
    return comparison


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _fmt(value: Any, pct: bool = False) -> str:
    if _is_missing(value):
        return ""
    if isinstance(value, float):
        if pct:
            return f"{value * 100:.2f}%"
        return f"{value:.4f}"
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            k: (
                None if _is_missing(v)
                else v.isoformat() if isinstance(v, date)
                else round(v, 6) if isinstance(v, float)
                else v
            )
            for k, v in row.items()
        }
        for row in rows
    ]


def _render_markdown(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    industry_info: dict[str, Any],
    dupont_rows: list[dict[str, Any]],
    indicator_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    benchmark: dict[str, Any] | None,
    bench_dupont: list[dict[str, Any]],
    comparison: list[dict[str, Any]],
) -> str:
    lines = [
        "# look-07 ROE & Capital Return (DuPont Analysis)",
        "",
        f"- stock: {stock}",
        f"- as_of_date: {as_of_date.isoformat()}",
        f"- lookback_years: {lookback_years}",
        f"- company_type: {profile.comp_type_label} ({profile.comp_type or 'unknown'})",
    ]
    if industry_info:
        lines.append(
            f"- industry: {industry_info.get('l1_name', '')} > "
            f"{industry_info.get('l2_name', '')} > "
            f"{industry_info.get('l3_name', '')} "
            f"({industry_info.get('peer_group_size', '?')} peers)"
        )

    # Summary
    lines.extend(["", "## Summary", ""])
    lines.append(f"- years_returned: {summary['years_returned']}")
    lines.append(f"- roe_latest (DuPont): {_fmt(summary.get('roe_latest'), pct=True)}")
    lines.append(f"- roe_driver: **{summary['roe_driver']}**")
    lines.append(f"- roe_trend: {summary['roe_trend']}")
    lines.append(f"- npm_latest: {_fmt(summary.get('npm_latest'), pct=True)}")
    lines.append(f"- asset_turnover_latest: {_fmt(summary.get('at_latest'))}")
    lines.append(f"- equity_multiplier_latest: {_fmt(summary.get('em_latest'))}")

    # DuPont Decomposition table
    lines.extend(["", "## DuPont Decomposition", ""])
    lines.append("ROE = NPM × Asset Turnover × Equity Multiplier")
    lines.append("")
    dp_cols = [
        "end_date", "n_income_attr_p", "revenue",
        "npm", "asset_turnover", "equity_multiplier", "roe_dupont",
    ]
    dp_headers = [
        "Year", "Net Income (Parent)", "Revenue",
        "NPM", "Asset Turnover", "Equity Multiplier", "ROE (DuPont)",
    ]
    lines.append("| " + " | ".join(dp_headers) + " |")
    lines.append("|" + "|".join("---" for _ in dp_headers) + "|")
    for row in dupont_rows:
        cells = []
        for c in dp_cols:
            v = row.get(c)
            if c in ("npm", "roe_dupont"):
                cells.append(_fmt(v, pct=True))
            else:
                cells.append(_fmt(v))
        lines.append("| " + " | ".join(cells) + " |")

    # Driver Analysis
    lines.extend(["", "## ROE Driver Analysis", ""])
    driver = summary["roe_driver"]
    if driver == "profitability-driven":
        lines.append("高ROE主要来自**较高的销售净利润率**，盈利能力突出，杠杆水平适中。这是最健康的ROE来源。")
    elif driver == "leverage-driven":
        lines.append("高ROE主要来自**较高的权益乘数（杠杆）**，销售净利润率偏低。需要关注债务风险，ROE质量存疑。")
    elif driver == "turnover-driven":
        lines.append("高ROE主要来自**较高的资产周转率**，属于薄利多销模式。盈利能力一般，但经营效率突出。")
    elif driver == "negative-equity":
        lines.append("当前处于**资不抵债状态**（归母净资产为负），杜邦分解完全失效。这是极端财务风险信号，应高度警惕。")
    elif driver == "negative-roe":
        lines.append("当前处于**亏损状态**（ROE为负），杜邦分解因子分析意义有限，应重点关注扭亏路径。")
    elif driver == "mixed":
        lines.append("ROE由多个因素共同驱动，未呈现单一主导特征。")
    else:
        lines.append("数据不足，无法判断ROE驱动类型。")

    # Indicator cross-check table
    if indicator_rows:
        lines.extend(["", "## Indicator Cross-Check (fin_indicator)", ""])
        ind_cols = ["end_date", "roe", "roe_dt", "roa", "netprofit_margin", "assets_turn", "debt_to_assets"]
        lines.append("| " + " | ".join(ind_cols) + " |")
        lines.append("|" + "|".join("---" for _ in ind_cols) + "|")
        for row in indicator_rows:
            lines.append("| " + " | ".join(_fmt(row.get(c)) for c in ind_cols) + " |")

    # Benchmark comparison
    lines.extend(["", "## Benchmark Comparison", ""])
    if benchmark:
        lines.append(f"- benchmark: {benchmark['name']} ({benchmark['ts_code']})")
        lines.append(f"- benchmark_total_mv: {_fmt(benchmark['total_mv'])} (万元)")
        lines.append(f"- mv_trade_date: {benchmark.get('mv_trade_date', '')}")

        if comparison:
            lines.append("")
            comp_cols = [
                "end_date",
                "target_roe_dupont", "bench_roe_dupont",
                "target_npm", "bench_npm",
                "target_asset_turnover", "bench_asset_turnover",
                "target_equity_multiplier", "bench_equity_multiplier",
            ]
            comp_headers = [
                "Year",
                "Target ROE", "Bench ROE",
                "Target NPM", "Bench NPM",
                "Target AT", "Bench AT",
                "Target EM", "Bench EM",
            ]
            lines.append("| " + " | ".join(comp_headers) + " |")
            lines.append("|" + "|".join("---" for _ in comp_headers) + "|")
            for row in comparison:
                cells = []
                for c in comp_cols:
                    v = row.get(c)
                    if "roe" in c or "npm" in c:
                        cells.append(_fmt(v, pct=True))
                    else:
                        cells.append(_fmt(v))
                lines.append("| " + " | ".join(cells) + " |")
    else:
        lines.append("- benchmark: not available (no SW L3 peers or market cap data)")

    return "\n".join(lines)


def _render_json(
    stock: str,
    as_of_date: date,
    lookback_years: int,
    profile: CompanyProfile,
    industry_info: dict[str, Any],
    dupont_rows: list[dict[str, Any]],
    indicator_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    benchmark: dict[str, Any] | None,
    bench_dupont: list[dict[str, Any]],
    comparison: list[dict[str, Any]],
) -> str:
    status = "ready" if summary["years_returned"] > 0 else "no-data"
    payload = {
        "rule_id": "look-07",
        "status": status,
        "stock": stock,
        "as_of_date": as_of_date.isoformat(),
        "lookback_years": lookback_years,
        "company_profile": profile.to_payload(),
        "industry_info": industry_info,
        "summary": summary,
        "dupont_rows": _serialize_rows(dupont_rows),
        "indicator_rows": _serialize_rows(indicator_rows),
        "benchmark": benchmark,
        "benchmark_dupont_rows": _serialize_rows(bench_dupont),
        "comparison": _serialize_rows(comparison),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run look-07 ROE & capital return DuPont analysis"
    )
    parser.add_argument("--stock", required=True)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--lookback-years", type=int, default=5)
    parser.add_argument("--db-path", default=str(default_db_path()))
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args()

    if args.lookback_years <= 0:
        raise SystemExit("--lookback-years must be a positive integer")

    as_of_date = parse_date(args.as_of_date)
    db_path = Path(args.db_path).expanduser().resolve()

    with connect_read_only(db_path) as con:
        profile = detect_company_profile(con, args.stock, as_of_date)

        if profile.is_financial:
            payload = {
                "rule_id": "look-07",
                "status": "not-applicable",
                "stock": args.stock,
                "as_of_date": as_of_date.isoformat(),
                "lookback_years": args.lookback_years,
                "company_profile": profile.to_payload(),
                "warning": profile.warning,
                "reason": "金融类公司杠杆结构与一般工商业差异过大，杜邦分解结果不可直接类比。",
            }
            if args.format == "json":
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                print("\n".join([
                    "# look-07 ROE & Capital Return (DuPont Analysis)",
                    "",
                    "## Not Applicable",
                    f"- stock: {args.stock}",
                    f"- warning: {profile.warning or ''}",
                    f"- reason: {payload['reason']}",
                ]))
            return

        # Fetch data
        raw_rows = _fetch_dupont_inputs(con, args.stock, as_of_date, args.lookback_years)
        indicator_rows = _fetch_indicator_rows(con, args.stock, as_of_date, args.lookback_years)
        industry_info = _fetch_peer_industry_info(con, args.stock)

        # Benchmark
        benchmark = _find_benchmark(con, args.stock, as_of_date)
        bench_raw = []
        if benchmark:
            bench_raw = _fetch_dupont_inputs(con, benchmark["ts_code"], as_of_date, args.lookback_years)

    # Compute
    dupont_rows = _compute_dupont(raw_rows, args.lookback_years)
    bench_dupont = _compute_dupont(bench_raw, args.lookback_years) if benchmark else []
    summary = _build_summary(dupont_rows)
    comparison = _build_comparison(dupont_rows, bench_dupont) if benchmark else []

    render_args = (
        args.stock, as_of_date, args.lookback_years, profile,
        industry_info, dupont_rows, indicator_rows, summary,
        benchmark, bench_dupont, comparison,
    )

    if args.format == "json":
        print(_render_json(*render_args))
    else:
        print(_render_markdown(*render_args))


if __name__ == "__main__":
    main()
