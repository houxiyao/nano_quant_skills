"""Tushare API fetcher with lazy client and retry logic."""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import pandas as pd
from loguru import logger
import tushare

if TYPE_CHECKING:
    from tushare.pro.client import DataApi as TushareProClient


class TushareError(Exception):
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.context = context or {}


class TushareFetcher:
    """Manages Tushare API connection and data fetching."""

    def __init__(self, token: str):
        self._token = token
        self._client: Optional[TushareProClient] = None

    @property
    def client(self) -> TushareProClient:
        if self._client is None:
            if not self._token:
                raise TushareError(
                    "Tushare token is required. Set it in config.yaml under tushare.token."
                )
            self._client = tushare.pro_api(token=self._token)
        return self._client

    def fetch(
        self,
        endpoint: str,
        method: str = "query",
        kwargs: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> pd.DataFrame:
        """Fetch data from Tushare with retry logic."""
        kw = kwargs or {}
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                if method == "query":
                    df = self.client.query(endpoint, **kw)
                else:
                    if not hasattr(self.client, method):
                        raise TushareError(f"Unsupported tushare method: {method}")
                    df = getattr(self.client, method)(**kw)
                if not isinstance(df, pd.DataFrame):
                    raise TushareError("Tushare did not return a DataFrame")
                if df.empty:
                    return df
                return df.copy()
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries:
                    wait = 2.0 * attempt
                    logger.warning(
                        f"Fetch failed for {endpoint}, attempt {attempt}/{max_retries}, "
                        f"retry in {wait:.1f}s: {exc}"
                    )
                    time.sleep(wait)
        raise TushareError(
            f"Fetch failed after {max_retries} retries: {last_exc}",
            {"endpoint": endpoint, "kwargs": kw},
        )

    def get_trade_dates(self, start: str, end: str) -> List[str]:
        """Get open trading dates between start and end."""
        cal = self.client.query("trade_cal", start_date=start, end_date=end)
        if cal is None or cal.empty:
            return []
        open_days = cal[cal["is_open"] == 1]["cal_date"].astype(str).tolist()
        return sorted(open_days)

    @staticmethod
    def get_report_periods(start: str, end: str) -> List[str]:
        """Generate quarterly report periods between start and end."""
        s = pd.to_datetime(start, format="%Y%m%d")
        e = pd.to_datetime(end, format="%Y%m%d")
        return sorted(pd.date_range(start=s, end=e, freq="QE-DEC").strftime("%Y%m%d").tolist())

    def resolve_trade_date_end(
        self,
        end_date: Optional[str] = None,
        publish_cutoff_hour: int = 18,
        disable_safe: bool = False,
    ) -> str:
        """Resolve safe end date respecting publish cutoff."""
        if end_date:
            return self.normalize_date(end_date)

        now = datetime.now()
        today = now.strftime("%Y%m%d")
        if disable_safe:
            return today

        if now.hour >= publish_cutoff_hour:
            return today

        lookback = (now - timedelta(days=14)).strftime("%Y%m%d")
        open_days = self.get_trade_dates(lookback, today)
        prior = [d for d in open_days if d < today]
        if prior:
            safe_end = prior[-1]
            logger.info(
                f"Safe trade date applied: today={today}, effective_end={safe_end}, "
                f"cutoff_hour={publish_cutoff_hour}"
            )
            return safe_end
        return today

    @staticmethod
    def normalize_date(text: str) -> str:
        """Normalize date string to YYYYMMDD format."""
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y%m%d")
            except ValueError:
                continue
        raise TushareError("Invalid date format. Use YYYYMMDD or YYYY-MM-DD", {"date": text})
