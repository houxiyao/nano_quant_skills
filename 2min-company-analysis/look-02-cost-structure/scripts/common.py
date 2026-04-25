from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys

import duckdb


_SHARED_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "seven-look-eight-question" / "scripts"
if str(_SHARED_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_SCRIPTS_DIR))

from eight_questions_domain import project_root as _shared_project_root


REPORT_TYPE = "1"
FINANCIAL_COMP_TYPES = {"2", "3", "4"}
COMPANY_TYPE_LABELS = {
    "1": "一般工商业",
    "2": "银行",
    "3": "保险",
    "4": "证券",
}


@dataclass(frozen=True)
class CompanyProfile:
    stock: str
    comp_type: str | None
    comp_type_label: str
    source_table: str | None
    latest_end_date: date | None
    visible_date: date | None

    @property
    def is_financial(self) -> bool:
        return self.comp_type in FINANCIAL_COMP_TYPES

    @property
    def warning(self) -> str | None:
        if not self.is_financial:
            return None
        return (
            f"目标公司属于金融类公司（comp_type={self.comp_type}，{self.comp_type_label}），"
            "当前 look-02-cost-structure skill 不适用。"
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "stock": self.stock,
            "comp_type": self.comp_type,
            "comp_type_label": self.comp_type_label,
            "source_table": self.source_table,
            "latest_end_date": self.latest_end_date.isoformat() if self.latest_end_date else None,
            "visible_date": self.visible_date.isoformat() if self.visible_date else None,
            "is_financial": self.is_financial,
        }


def project_root() -> Path:
    return _shared_project_root()


def default_db_path() -> Path:
    return project_root() / "data" / "ashare.duckdb"


def parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def connect_read_only(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open DuckDB with optional logical→physical table-name shims.

    When env-var SEVEN_LOOK_DB_TABLE_MAP is set to a JSON dict that maps
    logical table names to physical table names, opens an in-memory DuckDB,
    attaches the physical file read-only, and creates transparent VIEW shims
    so that all downstream SQL can use logical names without modification.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    raw_map = os.environ.get("SEVEN_LOOK_DB_TABLE_MAP", "")
    table_map: dict[str, str] = {}
    if raw_map:
        try:
            loaded = json.loads(raw_map)
            if isinstance(loaded, dict):
                table_map = {str(k): str(v) for k, v in loaded.items() if k != v}
        except Exception:  # noqa: BLE001
            pass

    if not table_map:
        return duckdb.connect(str(db_path), read_only=True)

    def _quote_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    # In-memory DB + ATTACH: create VIEW shims so queries using logical names work.
    con = duckdb.connect(":memory:")
    safe_path = str(db_path).replace("'", "''")
    con.execute(f"ATTACH '{safe_path}' AS physdb (READ_ONLY)")
    phys_tables = {
        str(r[0])
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_catalog = 'physdb' AND table_schema = 'main'"
        ).fetchall()
    }

    # Base pass: mirror all physical tables under same names.
    for phys_name in sorted(phys_tables):
        ident = _quote_ident(phys_name)
        con.execute(f"CREATE VIEW {ident} AS SELECT * FROM physdb.{ident}")

    # Override pass: map logical names to configured physical names.
    for logical_name, physical_name in table_map.items():
        if physical_name not in phys_tables:
            continue
        logical_ident = _quote_ident(logical_name)
        physical_ident = _quote_ident(physical_name)
        con.execute(
            f"CREATE OR REPLACE VIEW {logical_ident} AS SELECT * FROM physdb.{physical_ident}"
        )
    return con


def detect_company_profile(
    con: duckdb.DuckDBPyConnection,
    stock: str,
    as_of_date: date,
) -> CompanyProfile:
    base_query = """
    WITH candidates AS (
        SELECT
            'fin_income' AS source_table,
            comp_type,
            end_date,
            COALESCE(f_ann_date, ann_date, end_date) AS visible_date
        FROM fin_income
        WHERE ts_code = ? AND report_type = ?

        UNION ALL

        SELECT
            'fin_balance' AS source_table,
            comp_type,
            end_date,
            COALESCE(f_ann_date, ann_date, end_date) AS visible_date
        FROM fin_balance
        WHERE ts_code = ? AND report_type = ?

        UNION ALL

        SELECT
            'fin_cashflow' AS source_table,
            comp_type,
            end_date,
            COALESCE(f_ann_date, ann_date, end_date) AS visible_date
        FROM fin_cashflow
        WHERE ts_code = ? AND report_type = ?
    )
    SELECT
        source_table,
        comp_type,
        end_date,
        visible_date
    FROM candidates
    {where_clause}
    ORDER BY visible_date DESC NULLS LAST, end_date DESC NULLS LAST, source_table
    LIMIT 1
    """
    params = [stock, REPORT_TYPE, stock, REPORT_TYPE, stock, REPORT_TYPE]
    row = con.execute(
        base_query.format(where_clause="WHERE visible_date <= CAST(? AS DATE)"),
        params + [as_of_date],
    ).fetchone()
    if row is None:
        row = con.execute(base_query.format(where_clause=""), params).fetchone()

    if row is None:
        return CompanyProfile(
            stock=stock,
            comp_type=None,
            comp_type_label="未知",
            source_table=None,
            latest_end_date=None,
            visible_date=None,
        )

    source_table, comp_type, latest_end_date, visible_date = row
    return CompanyProfile(
        stock=stock,
        comp_type=comp_type,
        comp_type_label=COMPANY_TYPE_LABELS.get(comp_type, "未知"),
        source_table=source_table,
        latest_end_date=latest_end_date,
        visible_date=visible_date,
    )