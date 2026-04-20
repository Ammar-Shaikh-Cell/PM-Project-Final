"""Database writer module (stub placeholder for persistence)."""

from __future__ import annotations

from datetime import datetime
import logging

from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base

import config

Base = declarative_base()
# SQLite used for now, replace DB_CONNECTION_STRING in config for production DB
engine = create_engine(config.DB_CONNECTION_STRING)


# stores calculated features for every processed window
class WindowFeatures(Base):
    """Persisted feature snapshot for one processed rolling window."""

    __tablename__ = "window_features"

    id = Column(Integer, primary_key=True, autoincrement=True)
    window_start = Column(DateTime)
    window_end = Column(DateTime)
    screw_speed_mean = Column(Float)
    screw_speed_std = Column(Float)
    screw_speed_trend = Column(Float)
    pressure_mean = Column(Float)
    pressure_std = Column(Float)
    pressure_trend = Column(Float)
    temperature_mean = Column(Float)
    temperature_std = Column(Float)
    temperature_trend = Column(Float)
    load_mean = Column(Float)
    load_std = Column(Float)
    load_trend = Column(Float)
    pressure_per_rpm = Column(Float)
    temp_spread = Column(Float)
    load_per_pressure = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)


# stores confirmed machine state for every cycle
class MachineState(Base):
    """Persisted state detection output for one monitoring cycle."""

    __tablename__ = "machine_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    window_start = Column(DateTime)
    window_end = Column(DateTime)
    # what state was detected this cycle
    candidate_state = Column(String)
    # only filled when 3 consecutive windows agree
    confirmed_state = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# creates tables if they don't exist yet
Base.metadata.create_all(engine)


class DBWriter:
    """Database helper for storing features and machine states."""

    def __init__(self) -> None:
        """Initialize a reusable database engine for all writes."""
        # single engine instance reused across all writes
        self.engine = create_engine(config.DB_CONNECTION_STRING)

    def save_features(self, features: dict) -> None:
        """Save one feature dictionary row into the WindowFeatures table."""
        # called after every successful feature calculation
        session = Session(self.engine)
        try:
            feature_row = WindowFeatures(
                window_start=features.get("window_start"),
                window_end=features.get("window_end"),
                screw_speed_mean=features.get("screw_speed_mean"),
                screw_speed_std=features.get("screw_speed_std"),
                screw_speed_trend=features.get("screw_speed_trend"),
                pressure_mean=features.get("pressure_mean"),
                pressure_std=features.get("pressure_std"),
                pressure_trend=features.get("pressure_trend"),
                temperature_mean=features.get("temperature_mean"),
                temperature_std=features.get("temperature_std"),
                temperature_trend=features.get("temperature_trend"),
                load_mean=features.get("load_mean"),
                load_std=features.get("load_std"),
                load_trend=features.get("load_trend"),
                pressure_per_rpm=features.get("pressure_per_rpm"),
                temp_spread=features.get("temp_spread"),
                load_per_pressure=features.get("load_per_pressure"),
            )
            session.add(feature_row)
            session.commit()
            logging.info(
                "✅ Features saved for window: %s → %s",
                features.get("window_start"),
                features.get("window_end"),
            )
        except Exception as exc:  # pragma: no cover - runtime DB safety
            session.rollback()
            logging.warning("Failed to save features: %s", exc)
        finally:
            session.close()

    def save_state(self, window_start, window_end, candidate_state, confirmed_state) -> None:
        """Save candidate/confirmed state output for one cycle."""
        # called after every state detection cycle
        session = Session(self.engine)
        try:
            state_row = MachineState(
                window_start=window_start,
                window_end=window_end,
                candidate_state=candidate_state,
                confirmed_state=confirmed_state,
            )
            session.add(state_row)
            session.commit()
            logging.info(
                "✅ State saved: candidate=%s confirmed=%s",
                candidate_state,
                confirmed_state,
            )
        except Exception as exc:  # pragma: no cover - runtime DB safety
            session.rollback()
            logging.warning("Failed to save state: %s", exc)
        finally:
            session.close()

    def get_latest_state(self):
        """Return the most recently saved machine state row, if available."""
        # useful for API or dashboard to read current machine state
        session = Session(self.engine)
        try:
            return session.query(MachineState).order_by(MachineState.created_at.desc()).first()
        finally:
            session.close()
