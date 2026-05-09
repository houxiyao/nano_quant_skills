"""scoring 包 — 红旗提取、质量评分、建议生成。"""

from .quality_score import compute_quality_score, generate_commentary
from .recommendations import collect_human_requests, generate_recommendations
from .red_flags import collect_all_flags

__all__ = [
    "collect_all_flags",
    "compute_quality_score",
    "generate_commentary",
    "collect_human_requests",
    "generate_recommendations",
]
