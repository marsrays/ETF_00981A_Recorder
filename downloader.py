"""
Scheduled downloader for ETF portfolio xlsx files.
Downloads from ezmoney and saves to ./data/ if not already present.
"""
import re
import logging
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

DOWNLOAD_URL = "https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode=49YTW"
DATA_DIR = Path(__file__).parent / "data"

_FILENAME_RE = re.compile(r'filename=([^\s;]+)', re.IGNORECASE)
_DATE_RE = re.compile(r'(\d{8})')


def _filename_from_header(resp: requests.Response) -> str | None:
    """
    Extract filename from Content-Disposition header.
    e.g. 'attachment; filename=ETF_Investment_Portfolio_20260416.xlsx'
    """
    cd = resp.headers.get("content-disposition", "")
    m = _FILENAME_RE.search(cd)
    if not m:
        return None
    # Strip surrounding quotes if present
    return m.group(1).strip().strip('"\'')


def download_latest(store=None) -> dict:
    """
    Download the latest xlsx. Returns a status dict.

    Filename (and therefore date) is determined from the Content-Disposition
    header returned by the server — no need to open the file to find the date.
    If `store` is provided, registers the file in the store on success.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading from {DOWNLOAD_URL}")
    try:
        resp = requests.get(DOWNLOAD_URL, timeout=30, verify=False, headers={
            "User-Agent": "Mozilla/5.0 (compatible; ETFTracker/1.0)"
        })
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        return {"success": False, "error": str(e)}

    # --- Determine filename from header ---
    filename = _filename_from_header(resp)
    if not filename:
        logger.error("No filename in Content-Disposition header")
        return {"success": False, "error": "Server did not return a filename in Content-Disposition header"}

    # Validate it looks like our expected pattern
    if not _DATE_RE.search(filename):
        logger.error(f"Filename '{filename}' does not contain a date")
        return {"success": False, "error": f"Unexpected filename format: {filename}"}

    dest_path = DATA_DIR / filename

    # --- Skip if already downloaded ---
    if dest_path.exists():
        logger.info(f"Already have {filename}, skipping.")
        return {
            "success": True,
            "skipped": True,
            "reason": "File already exists",
            "filename": filename,
        }

    # --- Save file ---
    dest_path.write_bytes(resp.content)
    logger.info(f"Saved new file: {filename}")

    # --- Register in store ---
    newly_added = False
    if store is not None:
        try:
            newly_added = store.add_file(dest_path)
        except Exception as e:
            logger.warning(f"Store.add_file failed: {e}")

    return {
        "success": True,
        "skipped": False,
        "filename": filename,
        "registered_in_store": newly_added,
    }
