from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, create_engine, Session

DB_URL = "sqlite:///./swellscope.db"
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})

class WaveRecord(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime = Field(index=True)      # UTC

    # Ondas
    hs: Optional[float] = None            # wave height (m)
    tp: Optional[float] = None            # wave period (s)
    dp: Optional[float] = None            # wave direction (deg)

    # Mar/tempo
    sst: Optional[float] = None           # sea surface temp (°C)
    air_temp: Optional[float] = None      # 2m air temp (°C)
    wind_speed: Optional[float] = None    # 10m wind speed (m/s)
    wind_dir: Optional[float] = None      # 10m wind direction (deg)

    source: Optional[str] = None          # "open-meteo"
    location: Optional[str] = None        # "Leme-RJ"

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session() -> Session:
    return Session(engine)
