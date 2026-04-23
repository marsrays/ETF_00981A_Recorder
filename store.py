"""
In-memory + disk repository for ETF portfolio snapshots.
Each ETF gets its own PortfolioStore instance, loading from ./data/{fund_code}/
"""
from datetime import date
from pathlib import Path
from threading import Lock
from typing import Optional

from parser import parse_file, extract_date_from_filename

DATA_DIR = Path(__file__).parent / "data"


class PortfolioStore:
    def __init__(self, fund_code: str):
        self.fund_code = fund_code
        self._store: dict[str, dict] = {}
        self._sorted_dates: list[str] = []
        self._lock = Lock()
        self._fund_dir = DATA_DIR / fund_code
        self._fund_dir.mkdir(parents=True, exist_ok=True)
        self._load_all()

    def _load_all(self):
        for xlsx in self._fund_dir.glob("ETF_Investment_Portfolio_*.xlsx"):
            file_date = extract_date_from_filename(xlsx.name)
            if file_date is None:
                continue
            key = file_date.isoformat()
            with self._lock:
                if key in self._store:
                    continue
            try:
                data = parse_file(xlsx)
            except Exception as e:
                print(f"[{self.fund_code}] Failed to parse {xlsx.name}: {e}")
                continue
            with self._lock:
                if key not in self._store:
                    self._store[key] = data
                    self._refresh_sorted_locked()

    def _refresh_sorted_locked(self):
        self._sorted_dates = sorted(self._store.keys())

    def add_file(self, path: Path) -> bool:
        file_date = extract_date_from_filename(path.name)
        if file_date is None:
            raise ValueError(f"Cannot extract date from filename: {path.name}")
        key = file_date.isoformat()
        with self._lock:
            if key in self._store:
                return False
        data = parse_file(path)
        with self._lock:
            if key in self._store:
                return False
            self._store[key] = data
            self._refresh_sorted_locked()
            return True

    def all_dates(self) -> list[str]:
        with self._lock:
            return list(self._sorted_dates)

    def get(self, date_str: str) -> Optional[dict]:
        with self._lock:
            return self._store.get(date_str)

    def nearest_on_or_after(self, target: date) -> Optional[str]:
        t = target.isoformat()
        with self._lock:
            dates = list(self._sorted_dates)
        for d in dates:
            if d >= t:
                return d
        return None

    def nearest_on_or_before(self, target: date) -> Optional[str]:
        t = target.isoformat()
        with self._lock:
            dates = list(self._sorted_dates)
        for d in reversed(dates):
            if d <= t:
                return d
        return None

    def reload(self):
        self._load_all()
