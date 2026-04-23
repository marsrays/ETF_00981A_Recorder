"""
Scheduled downloader for ETF portfolio xlsx files.
Downloads from ezmoney and saves to ./data/{fund_code}/ if not already present.
"""
import re
import logging
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode={fund_code}"
DATA_DIR = Path(__file__).parent / "data"

ETF_CONFIGS = {
    "49YTW": {"name": "00981A", "url": BASE_URL.format(fund_code="49YTW")},
    "61YTW": {"name": "00988A", "url": BASE_URL.format(fund_code="61YTW")},
}

_FILENAME_RE = re.compile(r'filename=([^\s;]+)', re.IGNORECASE)
_DATE_RE = re.compile(r'(\d{8})')


def _filename_from_header(resp: requests.Response) -> str | None:
    cd = resp.headers.get("content-disposition", "")
    m = _FILENAME_RE.search(cd)
    if not m:
        return None
    return m.group(1).strip().strip('"\'')


def download_latest(fund_code: str, store=None) -> dict:
    """
    Download the latest xlsx for the given fund_code.
    Files are saved to ./data/{fund_code}/
    """
    if fund_code not in ETF_CONFIGS:
        return {"success": False, "error": f"Unknown fund_code: {fund_code}"}

    url = ETF_CONFIGS[fund_code]["url"]
    fund_dir = DATA_DIR / fund_code
    fund_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{fund_code}] Downloading from {url}")
    try:
        resp = requests.get(url, timeout=30, verify=False, headers={
            "User-Agent": "Mozilla/5.0 (compatible; ETFTracker/1.0)"
        })
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"[{fund_code}] Download failed: {e}")
        return {"success": False, "error": str(e)}

    filename = _filename_from_header(resp)
    if not filename:
        return {"success": False, "error": "Server did not return a filename in Content-Disposition header"}
    if not _DATE_RE.search(filename):
        return {"success": False, "error": f"Unexpected filename format: {filename}"}

    dest_path = fund_dir / filename

    if dest_path.exists():
        logger.info(f"[{fund_code}] Already have {filename}, skipping.")
        return {"success": True, "skipped": True, "reason": "File already exists", "filename": filename}

    dest_path.write_bytes(resp.content)
    logger.info(f"[{fund_code}] Saved new file: {filename}")

    newly_added = False
    if store is not None:
        try:
            newly_added = store.add_file(dest_path)
        except Exception as e:
            logger.warning(f"[{fund_code}] Store.add_file failed: {e}")

    return {"success": True, "skipped": False, "filename": filename, "registered_in_store": newly_added}
