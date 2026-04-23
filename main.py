"""
ETF Portfolio Tracker API
Run: uvicorn main:app --reload

Routes are prefixed by fund code, e.g.:
  GET  /{fund}/dates
  GET  /{fund}/snapshot/{date}
  GET  /{fund}/diff?start=&end=
  POST /{fund}/download
  GET  /etfs          - list all configured ETFs
  GET  /health
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
from downloader import download_latest, ETF_CONFIGS
from store import PortfolioStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# One store per ETF
stores: dict[str, PortfolioStore] = {
    code: PortfolioStore(code) for code in ETF_CONFIGS
}

scheduler = AsyncIOScheduler(timezone="Asia/Taipei")


def _get_store(fund: str) -> PortfolioStore:
    if fund not in stores:
        raise HTTPException(status_code=404, detail=f"Unknown fund: {fund}. Available: {list(stores)}")
    return stores[fund]


async def _run_download(fund_code: str) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, partial(download_latest, fund_code, store=stores[fund_code])
    )


async def scheduled_download():
    logger.info("Scheduled download triggered for all ETFs")
    for code in ETF_CONFIGS:
        result = await _run_download(code)
        logger.info(f"[{code}] Download result: {result}")


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
    description="Tracks 00981A and 00988A ETF portfolios.",
    version="2.0.0",
    lifespan=lifespan,
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


# ------------------------------------------------------------------ #
# Meta                                                                 #
# ------------------------------------------------------------------ #

@app.get("/etfs", summary="List all configured ETFs")
def list_etfs():
    return {
        "etfs": [
            {"fund_code": code, "name": cfg["name"]}
            for code, cfg in ETF_CONFIGS.items()
        ]
    }


@app.get("/health", summary="Health check")
def health():
    return {
        "status": "ok",
        "etfs": {code: len(s.all_dates()) for code, s in stores.items()},
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
    }


# ------------------------------------------------------------------ #
# Per-fund routes                                                      #
# ------------------------------------------------------------------ #

@app.get("/{fund}/dates", summary="List available snapshot dates for a fund")
def list_dates(fund: str):
    return {"fund": fund, "dates": _get_store(fund).all_dates()}


@app.get("/{fund}/snapshot/{snapshot_date}", summary="Get a single portfolio snapshot")
def get_snapshot(fund: str, snapshot_date: str):
    data = _get_store(fund).get(snapshot_date)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No snapshot for {fund} on {snapshot_date}")
    return data


@app.get("/{fund}/diff", summary="Compare two portfolio snapshots")
def get_diff(
    fund: str,
    start: str = Query(..., description="Start date YYYY-MM-DD (falls forward if missing)"),
    end: str = Query(..., description="End date YYYY-MM-DD (falls back if missing)"),
):
    store = _get_store(fund)
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

    result = diff_portfolios(store.get(resolved_start), store.get(resolved_end))
    result["requested_start"] = start
    result["requested_end"] = end
    return result


@app.post("/{fund}/download", summary="Manually trigger a download for a fund")
async def trigger_download(fund: str):
    _get_store(fund)  # validate fund exists
    result = await _run_download(fund)
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result.get("error", "Download failed"))
    return result
