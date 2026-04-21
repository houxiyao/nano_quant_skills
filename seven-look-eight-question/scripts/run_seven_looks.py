"""run_seven_looks.py — 七看总编排脚本

依次执行 look-01 ~ look-07 七个独立分析脚本，收集中间 JSON，
汇总为一份综合财务质量报告，并附带量化评语与行动建议。

执行流程
--------
Phase 1（自动）: 运行 look-01/02/03/06/07（纯数据库，无需外部输入）
Phase 2（半自动）: 运行 look-04/05（若未提供年报文本包则标记 human-in-loop）
Phase 3（汇总）: 合并 7 份中间 JSON → 综合评估
Phase 4（评语）: 附加质量评分 + 最多 3 条行动建议

用法示例
--------
# 全自动（look-04/05 将提示需要年报文本）
python run_seven_looks.py --stock 000002.SZ --as-of-date 2025-04-30

# 提供年报文本包后全量执行
python run_seven_looks.py --stock 000002.SZ --as-of-date 2025-04-30 \
    --report-bundle-04 /tmp/vanke_reports.json \
    --report-bundle-05 /tmp/vanke_notes.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import textwrap
from datetime import date, datetime
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SKILLS_ROOT = Path(__file__).resolve().parents[2]  # .github/skills/
PROJECT_ROOT = SKILLS_ROOT.parents[1]               # repo root

LOOK_SPECS: list[dict[str, Any]] = [
    {
        "rule_id": "look-01",
        "title": "盈收与利润质量",
        "skill_dir": "look-01-profit-quality",
        "script": "look_01_profit_quality.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-02",
        "title": "费用成本结构",
        "skill_dir": "look-02-cost-structure",
        "script": "look_02_cost_structure.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-03",
        "title": "增长率趋势",
        "skill_dir": "look-03-growth-trend",
        "script": "look_03_growth_trend.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-04",
        "title": "业务构成与市场分布",
        "skill_dir": "look-04-business-market-distribution",
        "script": "look_04_business_market_distribution.py",
        "default_lookback": 3,
        "extra_args_key": "report_bundle_04",
    },
    {
        "rule_id": "look-05",
        "title": "资产负债健康度",
        "skill_dir": "look-05-balance-sheet-health",
        "script": "look_05_balance_sheet_health.py",
        "default_lookback": 3,
        "extra_args_key": "report_bundle_05",
    },
    {
        "rule_id": "look-06",
        "title": "投入产出效率",
        "skill_dir": "look-06-input-output-efficiency",
        "script": "look_06_input_output_efficiency.py",
        "default_lookback": 3,
        "extra_args_key": None,
    },
    {
        "rule_id": "look-07",
        "title": "收益率与资本回报",
        "skill_dir": "look-07-roe-capital-return",
        "script": "look_07_roe_capital_return.py",
        "default_lookback": 5,
        "extra_args_key": None,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_db_path() -> Path:
    return PROJECT_ROOT / "data" / "ashare.duckdb"


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _summary_dict(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    summary = data.get("summary")
    return summary if isinstance(summary, dict) else {}


def _is_leverage_trend_deteriorating(trend: str) -> bool:
    return trend in ("deteriorating", "rising")


def _get_look_04_evidence_counts(data: dict[str, Any]) -> tuple[int, int]:
    summary = _summary_dict(data)
    target = data.get("target_analysis", {})
    biz = _first_non_none(
        summary.get("business_composition_evidence_count"),
        target.get("business_composition_evidence_count"),
        0,
    )
    overseas = _first_non_none(
        summary.get("overseas_sales_evidence_count"),
        target.get("overseas_sales_evidence_count"),
        0,
    )
    return int(biz), int(overseas)


def _run_look(
    spec: dict[str, Any],
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    db_path: str,
    extra_args: dict[str, str | None],
) -> dict[str, Any]:
    """Run a single look script as a subprocess and return its JSON output."""
    script_path = SKILLS_ROOT / spec["skill_dir"] / "scripts" / spec["script"]
    if not script_path.exists():
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": f"Script not found: {script_path}",
        }

    lb = lookback_years if lookback_years is not None else spec["default_lookback"]
    cmd = [
        sys.executable,
        str(script_path),
        "--stock", stock,
        "--as-of-date", as_of_date,
        "--lookback-years", str(lb),
        "--db-path", db_path,
        "--format", "json",
    ]

    # Add extra arguments (report-bundle for look-04/05)
    extra_key = spec.get("extra_args_key")
    if extra_key and extra_args.get(extra_key):
        bundle_path = extra_args[extra_key]
        if spec["rule_id"] == "look-04":
            cmd.extend(["--report-bundle", bundle_path])
        elif spec["rule_id"] == "look-05":
            cmd.extend(["--report-bundle", bundle_path])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": "Execution timed out (120s)",
        }

    if result.returncode != 0:
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": result.stderr.strip()[:500],
        }

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {
            "rule_id": spec["rule_id"],
            "status": "error",
            "error": f"Invalid JSON output: {result.stdout[:200]}",
        }

    # Normalize status: scripts that don't set a status key are considered ready
    if "status" not in data:
        data["status"] = "ready"
    return data
# ---------------------------------------------------------------------------

# Red flag extractors: each returns a list of (flag_text, severity) tuples
# severity: "critical" | "warning"

def _extract_flags_01(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-01: profit quality red flags."""
    flags = []
    summary = _summary_dict(data)

    profit_positive_years = summary.get("profit_dedt_positive_years")
    total_years = summary.get("years_returned", 0)
    if profit_positive_years is not None and total_years > 0:
        if profit_positive_years == 0:
            flags.append(("扣非利润连续亏损", "critical"))
        elif profit_positive_years < total_years:
            flags.append((f"扣非利润仅{profit_positive_years}/{total_years}年为正", "warning"))

    ocf_positive = summary.get("operating_cashflow_positive_years")
    if ocf_positive is not None and total_years > 0:
        if ocf_positive == 0:
            flags.append(("经营现金流连续为负", "critical"))
        elif ocf_positive < total_years:
            flags.append((f"经营现金流仅{ocf_positive}/{total_years}年为正", "warning"))

    return flags


def _extract_flags_02(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-02: cost structure red flags."""
    flags = []
    summary = _summary_dict(data)

    mismatch_counts = summary.get("mismatch_counts", {})
    sales_mismatch = mismatch_counts.get("sales_exp_vs_revenue", 0)
    if sales_mismatch and sales_mismatch > 0:
        flags.append((f"销售费用增长但营收不增长（{sales_mismatch}次）", "warning"))

    return flags


def _extract_flags_03(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-03: growth trend red flags."""
    flags = []
    summary = _summary_dict(data)

    rev_cagr = summary.get("revenue_cagr")
    if rev_cagr is not None and rev_cagr < -0.05:
        flags.append(("营收CAGR为负，收入持续萎缩", "critical"))

    ni_cagr = summary.get("net_profit_cagr")
    if ni_cagr is not None and ni_cagr < -0.10:
        flags.append(("归母净利润CAGR大幅为负", "critical"))

    mode = summary.get("growth_mode_signal", "")
    if mode == "acquisition-assisted-or-mixed":
        flags.append(("增长可能含并购驱动成分", "warning"))

    return flags


def _extract_flags_04(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-04: business/market distribution flags."""
    flags = []
    status = data.get("status", "")
    if status in ("human-in-loop-required", "partial"):
        flags.append(("业务构成与市场分布数据不完整，需人工补充年报", "warning"))
    return flags


def _extract_flags_05(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-05: balance sheet health red flags."""
    flags = []
    summary = _summary_dict(data)
    rows = data.get("debt_solvency_rows", [])

    leverage_trend = summary.get("leverage_trend", "")
    if _is_leverage_trend_deteriorating(leverage_trend):
        flags.append(("杠杆水平持续恶化", "warning"))

    # Check debt_to_assets from the latest row
    if rows:
        latest = rows[0] if isinstance(rows, list) else {}
        dta = latest.get("debt_to_assets")
        if dta is not None and dta > 80:
            flags.append((f"资产负债率极高（{dta:.1f}%）", "critical"))

    hidden_status = summary.get("hidden_liability_status", "")
    if hidden_status == "human-in-loop-required":
        flags.append(("隐性负债未检测，需人工补充年报附注", "warning"))

    return flags


def _extract_flags_06(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-06: input-output efficiency red flags."""
    flags = []
    summary = _summary_dict(data)

    wc_trend = summary.get("wc_trend", "")
    if wc_trend == "deteriorating":
        flags.append(("营运资金效率持续恶化", "warning"))

    wc_per_rev = summary.get("wc_per_revenue_latest")
    if wc_per_rev is not None and wc_per_rev > 1.0:
        flags.append(("一元收入需要超过一元营运资金", "warning"))

    return flags


def _extract_flags_07(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Look-07: ROE & capital return red flags."""
    flags = []
    summary = _summary_dict(data)

    driver = summary.get("roe_driver", "")
    if driver == "negative-equity":
        flags.append(("资不抵债，杜邦分解完全失效", "critical"))
    elif driver == "negative-roe":
        flags.append(("ROE为负，处于亏损状态", "critical"))
    elif driver == "leverage-driven":
        flags.append(("高ROE主要靠杠杆驱动，非真实盈利能力", "warning"))

    trend = summary.get("roe_trend", "")
    if trend == "deteriorating":
        flags.append(("ROE持续恶化", "warning"))

    return flags


FLAG_EXTRACTORS = {
    "look-01": _extract_flags_01,
    "look-02": _extract_flags_02,
    "look-03": _extract_flags_03,
    "look-04": _extract_flags_04,
    "look-05": _extract_flags_05,
    "look-06": _extract_flags_06,
    "look-07": _extract_flags_07,
}


def _collect_all_flags(
    results: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    """Collect red flags from all 7 look results."""
    all_flags = []
    for rule_id, data in sorted(results.items()):
        status = data.get("status", "")
        if status in ("not-applicable", "error"):
            continue
        extractor = FLAG_EXTRACTORS.get(rule_id)
        if extractor:
            for flag_text, severity in extractor(data):
                all_flags.append({
                    "rule_id": rule_id,
                    "flag": flag_text,
                    "severity": severity,
                })
    return all_flags


def _compute_quality_score(flags: list[dict[str, str]]) -> dict[str, Any]:
    """Compute a simple quality score based on red flag counts.

    Scoring: start from 100, deduct 15 per critical, 5 per warning. Floor at 0.
    """
    score = 100
    criticals = [f for f in flags if f["severity"] == "critical"]
    warnings = [f for f in flags if f["severity"] == "warning"]
    score -= len(criticals) * 15
    score -= len(warnings) * 5
    score = max(score, 0)

    if score >= 80:
        grade = "A"
        label = "财务质量良好"
    elif score >= 60:
        grade = "B"
        label = "财务质量一般，存在部分隐患"
    elif score >= 40:
        grade = "C"
        label = "财务质量较差，多项红旗预警"
    else:
        grade = "D"
        label = "财务质量极差，建议高度警惕"

    return {
        "score": score,
        "grade": grade,
        "label": label,
        "critical_count": len(criticals),
        "warning_count": len(warnings),
    }


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _generate_recommendations(
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
) -> list[dict[str, str]]:
    """Generate up to 3 actionable next-step recommendations."""
    recs: list[dict[str, str]] = []

    # 1. If look-04 or look-05 need human input, recommend providing report bundles
    pending_human = []
    for rid in ("look-04", "look-05"):
        status = results.get(rid, {}).get("status", "")
        if status in ("human-in-loop-required", "partial"):
            pending_human.append(rid)
    if pending_human:
        recs.append({
            "action": "补充年报文本",
            "detail": (
                f"{'、'.join(pending_human)} 需要年报全文/附注文本才能完成分析。"
                "建议下载最近3年年报PDF，提取正文后以 --report-bundle 参数传入，"
                "以获得业务构成、市场分布和隐性负债的完整证据。"
            ),
            "priority": "high",
        })

    # 2. If critical flags exist, recommend deep-dive
    critical_rules = list({f["rule_id"] for f in flags if f["severity"] == "critical"})
    if critical_rules:
        rule_names = {s["rule_id"]: s["title"] for s in LOOK_SPECS}
        names = "、".join(f"{r}（{rule_names.get(r, r)}）" for r in sorted(critical_rules))
        recs.append({
            "action": "深入排查关键风险",
            "detail": (
                f"以下维度触发了严重红旗：{names}。"
                "建议逐项展开该维度的详细 Markdown 报告，"
                "结合管理层讨论与分析（MD&A）、审计报告意见、"
                "以及同行业对比数据进行交叉验证。"
            ),
            "priority": "high",
        })

    # 3. If look-07 shows leverage-driven or negative, suggest investigating capital structure
    roe_driver = _summary_dict(results.get("look-07", {})).get("roe_driver", "")
    if roe_driver in ("leverage-driven", "negative-roe", "negative-equity") and len(recs) < 3:
        recs.append({
            "action": "分析资本结构可持续性",
            "detail": (
                "当前ROE质量存在问题（"
                + {"leverage-driven": "杠杆驱动", "negative-roe": "亏损",
                   "negative-equity": "资不抵债"}.get(roe_driver, roe_driver)
                + "）。建议进一步分析：(1) 有息负债到期时间表，"
                "(2) 再融资能力评估，(3) 经营现金流能否覆盖利息支出。"
            ),
            "priority": "medium",
        })

    # 4. If growth is negative, suggest investigating turnaround potential
    rev_cagr = _summary_dict(results.get("look-03", {})).get("revenue_cagr")
    if rev_cagr is not None and rev_cagr < 0 and len(recs) < 3:
        recs.append({
            "action": "评估收入恢复可能性",
            "detail": (
                f"营收CAGR为{rev_cagr*100:.1f}%，处于收缩趋势。"
                "建议结合行业景气度数据、公司新业务布局、"
                "在手订单/合同负债变化来判断收入是否有望触底回升。"
            ),
            "priority": "medium",
        })

    # 5. If everything looks good, suggest valuation check
    if quality["grade"] in ("A", "B") and len(recs) < 3:
        recs.append({
            "action": "进入估值与股东结构分析",
            "detail": (
                "七看财务质量检查未发现严重问题。"
                "建议继续执行八问中的估值合理性（PE/PB/PS）"
                "和股东结构分析，判断当前价格是否具有安全边际。"
            ),
            "priority": "medium",
        })

    return recs[:3]


# ---------------------------------------------------------------------------
# Human-in-loop summary
# ---------------------------------------------------------------------------

def _collect_human_requests(results: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    """Collect all human-in-loop requests from the 7 look results."""
    requests = []

    # Look-04
    r04 = results.get("look-04", {})
    if r04.get("status") in ("human-in-loop-required", "partial"):
        human_reqs = r04.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-04",
                    "request": req if isinstance(req, str) else str(req),
                })
        else:
            requests.append({
                "rule_id": "look-04",
                "request": "请提供目标公司最近3年年报全文文本（JSON格式），用于提取业务构成、海外销售和客户集中度证据。",
            })

    # Look-05
    r05 = results.get("look-05", {})
    hidden_status = _summary_dict(r05).get("hidden_liability_status", "")
    if hidden_status == "human-in-loop-required":
        human_reqs = r05.get("human_in_loop_requests", [])
        if isinstance(human_reqs, list) and human_reqs:
            for req in human_reqs:
                requests.append({
                    "rule_id": "look-05",
                    "request": req if isinstance(req, str) else str(req),
                })
        else:
            requests.append({
                "rule_id": "look-05",
                "request": "请提供目标公司最近3年年报附注全文文本（JSON格式），用于提取隐性负债（对外担保、表外融资等）证据。",
            })

    return requests


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------

def _render_json(
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
    human_requests: list[dict[str, str]],
    recommendations: list[dict[str, str]],
    intermediate_dir: Path | None,
) -> str:
    commentary = _generate_commentary(stock, results, flags, quality)
    normalized_results = {
        rid: {
            "rule_id": rid,
            "title": next((s["title"] for s in LOOK_SPECS if s["rule_id"] == rid), rid),
            "status": data.get("status", "unknown"),
            "summary": _summary_dict(data),
        }
        for rid, data in sorted(results.items())
    }
    raw_results = {
        rid: data for rid, data in sorted(results.items())
    }
    payload = {
        "framework": "七看财务质量综合评估",
        "stock": stock,
        "as_of_date": as_of_date,
        "lookback_years": lookback_years,
        "quality_score": quality,
        "red_flags": flags,
        "commentary": commentary,
        "human_in_loop_requests": human_requests,
        "recommendations": recommendations,
        "results": normalized_results,
        "look_results": normalized_results,
        "raw_results": raw_results,
        "intermediate_files": (
            str(intermediate_dir) if intermediate_dir else None
        ),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def _render_markdown(
    stock: str,
    as_of_date: str,
    lookback_years: int | None,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
    human_requests: list[dict[str, str]],
    recommendations: list[dict[str, str]],
    intermediate_dir: Path | None,
) -> str:
    lines: list[str] = []

    # Header
    lines.append("# 七看财务质量综合评估报告")
    lines.append("")
    lines.append(f"- 股票代码: {stock}")
    lines.append(f"- 分析日期: {as_of_date}")
    lines.append(f"- 回看年数: {lookback_years or '各维度默认'}")
    lines.append("")

    # Quality score
    lines.append("## 综合质量评分")
    lines.append("")
    grade = quality["grade"]
    score = quality["score"]
    label = quality["label"]
    lines.append(f"**{grade} ({score}/100)** — {label}")
    lines.append("")
    lines.append(f"- 严重红旗: {quality['critical_count']} 项")
    lines.append(f"- 一般预警: {quality['warning_count']} 项")
    lines.append("")

    # Red flags
    if flags:
        lines.append("## 红旗预警清单")
        lines.append("")
        lines.append("| 维度 | 预警内容 | 严重程度 |")
        lines.append("|------|---------|---------|")
        severity_icon = {"critical": "🔴 严重", "warning": "🟡 警示"}
        rule_titles = {s["rule_id"]: s["title"] for s in LOOK_SPECS}
        for f in flags:
            rid = f["rule_id"]
            title = rule_titles.get(rid, rid)
            sev = severity_icon.get(f["severity"], f["severity"])
            lines.append(f"| {rid} {title} | {f['flag']} | {sev} |")
        lines.append("")

    # 7-dimension summary table
    lines.append("## 七看维度概览")
    lines.append("")
    lines.append("| # | 维度 | 状态 | 核心发现 |")
    lines.append("|---|------|------|---------|")
    for spec in LOOK_SPECS:
        rid = spec["rule_id"]
        data = results.get(rid, {})
        status = data.get("status", "未执行")
        finding = _summarize_one_look(rid, data)
        lines.append(f"| {rid} | {spec['title']} | {status} | {finding} |")
    lines.append("")

    # Human-in-loop
    if human_requests:
        lines.append("## 待人工补充信息")
        lines.append("")
        for i, req in enumerate(human_requests, 1):
            lines.append(f"{i}. **{req['rule_id']}**: {req['request']}")
        lines.append("")

    # Recommendations
    if recommendations:
        lines.append("## 下一步行动建议（最多3条）")
        lines.append("")
        priority_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}
        for i, rec in enumerate(recommendations, 1):
            icon = priority_icon.get(rec.get("priority", ""), "")
            lines.append(f"### {i}. {icon} {rec['action']}")
            lines.append("")
            lines.append(rec["detail"])
            lines.append("")

    # Commentary
    lines.append("## 量化评语")
    lines.append("")
    lines.append(_generate_commentary(stock, results, flags, quality))
    lines.append("")

    # Raw pass-through per look for auditability
    lines.append("## 分项原始分析透传")
    lines.append("")
    lines.append("以下内容为各分项脚本的原始 JSON 输出透传，用于复核汇总结论是否存在遗漏或失真。")
    lines.append("")
    for spec in LOOK_SPECS:
        rid = spec["rule_id"]
        data = results.get(rid, {})
        lines.append(f"### {rid} {spec['title']}")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(data, ensure_ascii=False, indent=2, default=str))
        lines.append("```")
        lines.append("")

    # Intermediate files
    if intermediate_dir:
        lines.append("## 中间文件")
        lines.append("")
        lines.append(f"7 份量化中间 JSON 已保存至: `{intermediate_dir}/`")
        lines.append("")

    return "\n".join(lines)


def _summarize_one_look(rule_id: str, data: dict[str, Any]) -> str:
    """Generate a one-line summary for each look dimension."""
    status = data.get("status", "")
    if status == "not-applicable":
        return "金融类公司，不适用"
    if status == "error":
        return f"执行出错: {data.get('error', '')[:60]}"
    if status in ("human-in-loop-required",):
        return "需人工补充年报文本"

    summary = _summary_dict(data)

    if rule_id == "look-01":
        pos = summary.get("profit_dedt_positive_years", "?")
        total = summary.get("years_returned", "?")
        ocf = summary.get("operating_cashflow_positive_years", "?")
        return f"扣非利润为正: {pos}/{total}年, 经营现金流为正: {ocf}/{total}年"

    if rule_id == "look-02":
        mismatch_counts = summary.get("mismatch_counts", {})
        mis = mismatch_counts.get("sales_exp_vs_revenue", 0)
        return f"销售费用/营收不匹配: {mis}次" if mis else "费用与营收匹配度正常"

    if rule_id == "look-03":
        rc = summary.get("revenue_cagr")
        nc = summary.get("net_profit_cagr")
        rc_str = f"{rc*100:.1f}%" if rc is not None else "N/A"
        nc_str = f"{nc*100:.1f}%" if nc is not None else "N/A"
        mode = summary.get("growth_mode_signal", "")
        return f"营收CAGR: {rc_str}, 净利润CAGR: {nc_str}, 模式: {mode}"

    if rule_id == "look-04":
        biz, overseas = _get_look_04_evidence_counts(data)
        return f"业务构成证据: {biz}条, 海外销售证据: {overseas}条"

    if rule_id == "look-05":
        trend = summary.get("leverage_trend", "")
        hidden = summary.get("hidden_liability_status", "")
        return f"杠杆趋势: {trend}, 隐性负债: {hidden}"

    if rule_id == "look-06":
        wc = summary.get("wc_per_revenue_latest")
        trend = summary.get("wc_trend", "")
        wc_str = f"{wc:.2f}" if wc is not None else "N/A"
        return f"WC/收入: {wc_str}, 趋势: {trend}"

    if rule_id == "look-07":
        driver = summary.get("roe_driver", "")
        trend = summary.get("roe_trend", "")
        roe = summary.get("roe_latest")
        roe_str = f"{roe*100:.2f}%" if roe is not None else "N/A"
        return f"ROE(DuPont): {roe_str}, 驱动: {driver}, 趋势: {trend}"

    return str(summary)[:80] if summary else "已完成"


def _generate_commentary(
    stock: str,
    results: dict[str, dict[str, Any]],
    flags: list[dict[str, str]],
    quality: dict[str, Any],
) -> str:
    """Generate a brief financial commentary based on all 7 dimensions."""
    parts: list[str] = []

    grade = quality["grade"]
    score = quality["score"]

    if grade == "A":
        parts.append(f"综合评分 {score}/100，七看维度整体表现良好，未发现明显财务质量隐患。")
    elif grade == "B":
        parts.append(f"综合评分 {score}/100，财务质量总体尚可，但存在个别需要关注的预警信号。")
    elif grade == "C":
        parts.append(f"综合评分 {score}/100，多个维度触发预警红旗，财务质量堪忧，建议审慎对待。")
    else:
        parts.append(f"综合评分 {score}/100，严重红旗密集，财务质量极差，强烈建议回避或做深度尽调。")

    # Profit quality
    r01 = _summary_dict(results.get("look-01", {}))
    ocf_pos = r01.get("operating_cashflow_positive_years")
    total_y = r01.get("years_returned")
    if ocf_pos is not None and total_y and ocf_pos < total_y:
        parts.append(f"利润质量方面，经营现金流仅{ocf_pos}年为正（共{total_y}年），利润含金量不足。")

    # Growth
    r03 = _summary_dict(results.get("look-03", {}))
    rev_cagr = r03.get("revenue_cagr")
    if rev_cagr is not None:
        if rev_cagr < -0.05:
            parts.append(f"增长方面，营收CAGR为{rev_cagr*100:.1f}%，处于明显收缩通道。")
        elif rev_cagr > 0.15:
            parts.append(f"增长方面，营收CAGR为{rev_cagr*100:.1f}%，保持较高增速。")

    # ROE
    r07 = _summary_dict(results.get("look-07", {}))
    driver = r07.get("roe_driver", "")
    if driver == "profitability-driven":
        parts.append("资本回报方面，ROE由盈利能力驱动，属于健康模式。")
    elif driver == "leverage-driven":
        parts.append("资本回报方面，ROE主要依赖杠杆，真实盈利能力有限，需警惕债务风险。")
    elif driver in ("negative-roe", "negative-equity"):
        parts.append("资本回报方面，公司处于亏损或资不抵债状态，需要高度关注。")

    # Balance sheet
    r05 = _summary_dict(results.get("look-05", {}))
    lev_trend = r05.get("leverage_trend", "")
    if _is_leverage_trend_deteriorating(lev_trend):
        parts.append("负债健康度方面，杠杆水平逐年攀升，偿债压力持续增大。")

    if not parts:
        parts.append("数据不足或公司类型不适用，无法生成有效评语。")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="七看财务质量综合评估 — 依次执行 look-01 ~ look-07 并汇总报告",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            示例:
              python run_seven_looks.py --stock 000002.SZ --as-of-date 2025-04-30
              python run_seven_looks.py --stock 000002.SZ --as-of-date 2025-04-30 \\
                  --report-bundle-04 reports.json --report-bundle-05 notes.json
        """),
    )
    parser.add_argument("--stock", required=True, help="股票代码, 如 000002.SZ")
    parser.add_argument("--as-of-date", default=None, help="分析日期 YYYY-MM-DD")
    parser.add_argument("--lookback-years", type=int, default=None,
                        help="统一回看年数（不设则每个维度用自己的默认值）")
    parser.add_argument("--db-path", default=str(_default_db_path()), help="DuckDB 路径")
    parser.add_argument("--report-bundle-04", default=None,
                        help="look-04 年报全文文本包（JSON）")
    parser.add_argument("--report-bundle-05", default=None,
                        help="look-05 年报附注文本包（JSON）")
    parser.add_argument("--output-dir", default=None,
                        help="中间文件输出目录（不设则使用临时目录）")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown",
                        help="输出格式")
    args = parser.parse_args()

    as_of_date = _parse_date(args.as_of_date).isoformat()

    # Determine output directory for intermediate files
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        import tempfile
        output_dir = Path(tempfile.mkdtemp(prefix=f"seven_looks_{args.stock}_"))

    output_dir.mkdir(parents=True, exist_ok=True)

    extra_args = {
        "report_bundle_04": args.report_bundle_04,
        "report_bundle_05": args.report_bundle_05,
    }

    # Phase 1 & 2: Run all 7 looks
    results: dict[str, dict[str, Any]] = {}
    auto_rules = ["look-01", "look-02", "look-03", "look-06", "look-07"]
    human_rules = ["look-04", "look-05"]

    print(f"[七看] 开始分析 {args.stock}，分析日期 {as_of_date}", file=sys.stderr)

    for spec in LOOK_SPECS:
        rid = spec["rule_id"]
        phase = "Phase 1 (自动)" if rid in auto_rules else "Phase 2 (半自动)"
        print(f"[七看] {phase} 执行 {rid}: {spec['title']} ...", file=sys.stderr)

        data = _run_look(
            spec=spec,
            stock=args.stock,
            as_of_date=as_of_date,
            lookback_years=args.lookback_years,
            db_path=args.db_path,
            extra_args=extra_args,
        )
        results[rid] = data

        # Write intermediate file
        intermediate_path = output_dir / f"{rid}.json"
        intermediate_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        status = data.get("status", "unknown")
        print(f"[七看]   → {rid} 完成, status={status}", file=sys.stderr)

    # Phase 3: Aggregate
    print("[七看] Phase 3: 汇总红旗与评分 ...", file=sys.stderr)
    flags = _collect_all_flags(results)
    quality = _compute_quality_score(flags)
    human_requests = _collect_human_requests(results)

    # Phase 4: Commentary + recommendations
    print("[七看] Phase 4: 生成评语与建议 ...", file=sys.stderr)
    recommendations = _generate_recommendations(results, flags, quality)

    render_args = (
        args.stock, as_of_date, args.lookback_years,
        results, flags, quality, human_requests, recommendations, output_dir,
    )

    if args.format == "json":
        print(_render_json(*render_args))
    else:
        print(_render_markdown(*render_args))

    print(f"\n[七看] 完成。中间文件已保存至: {output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
