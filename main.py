"""
ETF Portfolio Tracker API
Run: uvicorn main:app --reload

Endpoints:
  GET /dates              - list all available dates
  GET /snapshot/{date}    - get a single snapshot
  GET /diff               - diff between start_date and end_date (closest match)
  POST /download          - manually trigger a download
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from functools import partial
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from diff import diff_portfolios
from downloader import download_latest
from store import PortfolioStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

store = PortfolioStore()
scheduler = AsyncIOScheduler(timezone="Asia/Taipei")


async def _run_download() -> dict:
    """Run the blocking download_latest in a thread pool so the event loop stays free."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(download_latest, store=store))


async def scheduled_download():
    logger.info("Scheduled download triggered")
    result = await _run_download()
    logger.info(f"Download result: {result}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        scheduled_download,
        CronTrigger(hour=17, minute=0, timezone="Asia/Taipei"),
        id="daily_download",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started – daily download at 17:00 Asia/Taipei")
    yield
    scheduler.shutdown()


app = FastAPI(
    title="ETF Portfolio Tracker",
    description="Tracks 49YTW ETF portfolio changes over time.",
    version="1.0.0",
    lifespan=lifespan,
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


# ------------------------------------------------------------------ #
# Routes                                                               #
# ------------------------------------------------------------------ #

@app.get("/dates", summary="List all available snapshot dates")
def list_dates():
    """Returns all dates for which we have data, sorted ascending."""
    return {"dates": store.all_dates()}


@app.get("/snapshot/{snapshot_date}", summary="Get a single portfolio snapshot")
def get_snapshot(snapshot_date: str):
    """
    Get the portfolio snapshot for an exact date (YYYY-MM-DD).
    Returns 404 if that exact date is not available.
    """
    data = store.get(snapshot_date)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No snapshot for date {snapshot_date}")
    return data


@app.get("/diff", summary="Compare two portfolio snapshots")
def get_diff(
    start: str = Query(..., description="Start date YYYY-MM-DD (falls forward if missing)"),
    end: str = Query(..., description="End date YYYY-MM-DD (falls back if missing)"),
):
    """
    Returns the difference between the two nearest available snapshots.

    - **start**: if no data on this date, uses the next available date
    - **end**: if no data on this date, uses the previous available date
    """
    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must be in YYYY-MM-DD format")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start must be <= end")

    resolved_start = store.nearest_on_or_after(start_date)
    resolved_end = store.nearest_on_or_before(end_date)

    if resolved_start is None:
        raise HTTPException(status_code=404, detail=f"No data on or after {start}")
    if resolved_end is None:
        raise HTTPException(status_code=404, detail=f"No data on or before {end}")
    if resolved_start > resolved_end:
        raise HTTPException(
            status_code=404,
            detail=f"Resolved start ({resolved_start}) is after resolved end ({resolved_end}). No overlap.",
        )

    start_snapshot = store.get(resolved_start)
    end_snapshot = store.get(resolved_end)

    result = diff_portfolios(start_snapshot, end_snapshot)
    result["requested_start"] = start
    result["requested_end"] = end
    return result


@app.post("/download", summary="Manually trigger a download")
async def trigger_download():
    """Immediately downloads the latest xlsx and registers it if new."""
    result = await _run_download()
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result.get("error", "Download failed"))
    return result


@app.get("/health", summary="Health check")
def health():
    return {
        "status": "ok",
        "snapshots_loaded": len(store.all_dates()),
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
    }
