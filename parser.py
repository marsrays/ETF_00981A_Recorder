"""
Parse ETF_Investment_Portfolio_{DATE}.xlsx files into structured data.
"""
import re
import warnings
import pandas as pd
from datetime import date
from pathlib import Path

# ezmoney 產出的 xlsx 缺少 default style，openpyxl 會警告但不影響解析
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# Converts ROC date string "115/04/15" → date(2026, 4, 15)
def _roc_to_date(roc_str: str) -> date:
    parts = roc_str.strip().split("/")
    year = int(parts[0]) + 1911
    return date(year, int(parts[1]), int(parts[2]))


def _parse_ntd(val: str) -> int | None:
    if not isinstance(val, str):
        return None
    cleaned = val.replace("NTD", "").replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_pct(val: str) -> float | None:
    if not isinstance(val, str):
        return None
    try:
        return float(val.replace("%", "").strip()) / 100
    except ValueError:
        return None


def parse_file(path: Path) -> dict:
    """Parse a single xlsx file and return structured dict."""
    df = pd.read_excel(path, sheet_name=0, header=None, dtype=str)

    # --- date ---
    date_cell = str(df.iloc[0, 0])  # e.g. "資料日期：115/04/15"
    roc_date_str = re.search(r"(\d{3}/\d{2}/\d{2})", date_cell).group(1)
    record_date = _roc_to_date(roc_date_str)

    # --- fund assets (rows 3-5) ---
    nav_total = _parse_ntd(str(df.iloc[3, 1]))
    units_outstanding = int(str(df.iloc[4, 1]).replace(",", ""))
    nav_per_unit = float(re.sub(r"[^0-9.]", "", str(df.iloc[5, 1])))

    # --- asset allocation ---
    # Row 8: 期貨(名目本金), Row 9: 股票
    # Row 12: 現金, Row 13: 期貨保證金, Row 14: 申贖應付款, Row 15: 應收付證券款
    futures_nominal = _parse_ntd(str(df.iloc[8, 1]))
    equity_total = _parse_ntd(str(df.iloc[9, 1]))
    equity_weight = _parse_pct(str(df.iloc[9, 2]))
    cash = _parse_ntd(str(df.iloc[12, 1]))
    futures_margin = _parse_ntd(str(df.iloc[13, 1]))
    redemption_payable = _parse_ntd(str(df.iloc[14, 1]))
    securities_receivable = _parse_ntd(str(df.iloc[15, 1]))

    # --- stock holdings (row 20 = header, 21+ = data) ---
    stocks = []
    for i in range(20, len(df)):
        row = df.iloc[i]
        code = row[0]
        if pd.isna(code):
            continue
        code_str = code.strip()
        if not code_str:
            continue
        name = str(row[1]).strip()
        try:
            shares = int(str(row[2]).replace(",", ""))
        except (ValueError, TypeError):
            continue
        weight = _parse_pct(str(row[3]))
        stocks.append({
            "code": code_str,
            "name": name,
            "shares": shares,
            "weight": weight,
        })

    return {
        "date": record_date.isoformat(),
        "fund_assets": {
            "nav_total_ntd": nav_total,
            "units_outstanding": units_outstanding,
            "nav_per_unit_ntd": nav_per_unit,
        },
        "asset_allocation": {
            "futures_nominal_ntd": futures_nominal,
            "equity_total_ntd": equity_total,
            "equity_weight": equity_weight,
            "cash_ntd": cash,
            "futures_margin_ntd": futures_margin,
            "redemption_payable_ntd": redemption_payable,
            "securities_receivable_ntd": securities_receivable,
        },
        "stocks": stocks,
    }


def extract_date_from_filename(filename: str) -> date | None:
    """Extract date from ETF_Investment_Portfolio_YYYYMMDD.xlsx"""
    m = re.search(r"(\d{8})", filename)
    if not m:
        return None
    s = m.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
