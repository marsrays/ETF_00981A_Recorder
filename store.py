"""
In-memory + disk repository for ETF portfolio snapshots.
Loads all xlsx files from ./data/ on startup and indexes them by date.
"""
import json
from datetime import date
from pathlib import Path
from typing import Optional

from parser import parse_file, extract_date_from_filename

DATA_DIR = Path(__file__).parent / "data"


class PortfolioStore:
    def __init__(self):
        # date_str → parsed portfolio dict
        self._store: dict[str, dict] = {}
        self._sorted_dates: list[str] = []
        self._lock = Lock()  # 初始化鎖
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._load_all()

    # ------------------------------------------------------------------ #
    # Loading                                                              #
    # ------------------------------------------------------------------ #

    def _load_all(self):
        for xlsx in DATA_DIR.glob("ETF_Investment_Portfolio_*.xlsx"):
            file_date = extract_date_from_filename(xlsx.name)
            if file_date is None:
                continue
            key = file_date.isoformat()
            if key not in self._store:
                try:
                    data = parse_file(xlsx)
                    self._store[key] = data
                except Exception as e:
                    print(f"[store] Failed to parse {xlsx.name}: {e}")
        self._refresh_sorted()

    def _refresh_sorted(self):
        self._sorted_dates = sorted(self._store.keys())

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def add_file(self, path: Path) -> bool:
        """Parse and add a new xlsx file. Returns True if newly added."""
        file_date = extract_date_from_filename(path.name)
        if file_date is None:
            raise ValueError(f"Cannot extract date from filename: {path.name}")
        key = file_date.isoformat()
        if key in self._store:
            return False  # already exists
        data = parse_file(path)
        self._store[key] = data
        self._refresh_sorted()
        return True

    def all_dates(self) -> list[str]:
        return list(self._sorted_dates)

    def get(self, date_str: str) -> Optional[dict]:
        return self._store.get(date_str)

    def nearest_on_or_after(self, target: date) -> Optional[str]:
        """Find the closest date >= target."""
        t = target.isoformat()
        for d in self._sorted_dates:
            if d >= t:
                return d
        return None

    def nearest_on_or_before(self, target: date) -> Optional[str]:
        """Find the closest date <= target."""
        t = target.isoformat()
        for d in reversed(self._sorted_dates):
            if d <= t:
                return d
        return None

    def reload(self):
        self._load_all()
