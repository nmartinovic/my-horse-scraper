from datetime import datetime
from typing import Dict, Any, Optional
from sqlmodel import Field, SQLModel
from sqlalchemy import Column, JSON as SAJSON

class Race(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unibet_id: str
    name: str
    meeting: str
    race_time: datetime
    url: str
    surface: Optional[str] = None
    distance_m: Optional[int] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

class RaceDetail(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    race_id: int = Field(foreign_key="race.id")

    # pydantic sees a normal Python dict ...
    bookmarklet_json: Dict[str, Any] = Field(
        sa_column=Column(SAJSON)              # ... but SQLAlchemy still stores JSON
    )

    scraped_at: datetime = Field(default_factory=datetime.utcnow)

class ScrapeLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_type: str  # 'daily' | 'race'
    started_at: datetime
    finished_at: datetime
    status: str  # 'ok' | 'error'
    message: Optional[str] = None