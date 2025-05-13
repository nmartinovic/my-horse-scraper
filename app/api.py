from datetime import date
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from .db import engine
from .models import Race, RaceDetail
from .scrapers.daily import run_daily_scrape
from .scrapers.race import run_race_scrape

router = APIRouter(prefix="/api")


# ── Pydantic response models ─────────────────────────────────────────────
class RaceOut(BaseModel):
    id: int
    meeting: str
    name: str
    race_time: str  # ISO 8601
    unibet_id: int

    class Config:
        orm_mode = True


class RaceDetailOut(BaseModel):
    race: RaceOut
    bookmarklet_json: dict

    class Config:
        orm_mode = True


# ── POST /scrape/daily ───────────────────────────────────────────────────
@router.post(
    "/scrape/daily",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger the daily scrape now",
)
async def trigger_daily_scrape(tasks: BackgroundTasks):
    tasks.add_task(run_daily_scrape)
    return {"message": "Daily scrape scheduled"}


# ── POST /scrape/race/{race_id} ──────────────────────────────────────────
@router.post(
    "/scrape/race/{race_id}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a per-race scrape now (race_id is our DB id)",
)
async def trigger_race_scrape(race_id: int, tasks: BackgroundTasks):
    with Session(engine) as s:
        if not s.get(Race, race_id):
            raise HTTPException(status_code=404, detail="Race not found")
    tasks.add_task(run_race_scrape, race_id)
    return {"message": f"Race {race_id} scrape scheduled"}


# ── GET /races ───────────────────────────────────────────────────────────
@router.get(
    "/races",
    response_model=List[RaceOut],
    summary="List races (optionally filter by date)",
)
async def list_races(race_date: Optional[date] = None):
    with Session(engine) as s:
        stmt = select(Race)
        if race_date:
            stmt = stmt.where(Race.race_time.date() == race_date)
        return s.exec(stmt.order_by(Race.race_time)).all()


# ── GET /races/{id}/detail ───────────────────────────────────────────────
@router.get(
    "/races/{race_id}/detail",
    response_model=RaceDetailOut,
    summary="Get meta + bookmarklet JSON for one race",
)
async def get_race_detail(race_id: int):
    with Session(engine) as s:
        race = s.get(Race, race_id)
        if not race:
            raise HTTPException(status_code=404, detail="Race not found")

        detail = (
            s.exec(select(RaceDetail).where(RaceDetail.race_id == race_id)).first()
        )
        if not detail:
            raise HTTPException(
                status_code=404,
                detail="Race scraped, but bookmarklet detail not yet available",
            )

        return {"race": race, "bookmarklet_json": detail.bookmarklet_json}
