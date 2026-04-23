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


# stores historical baseline stats per feature per regime
# used as reference for live vs baseline comparison
class BaselineRegistry(Base):
    """Reference baseline statistics used for live feature evaluation."""

    __tablename__ = "baseline_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    regime_type = Column(String)
    profile_id = Column(Integer, nullable=True)
    feature_name = Column(String)
    mean_value = Column(Float)
    std_value = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    p10_value = Column(Float)
    p90_value = Column(Float)
    warning_low = Column(Float, nullable=True)
    warning_high = Column(Float, nullable=True)
    critical_low = Column(Float, nullable=True)
    critical_high = Column(Float, nullable=True)
    sample_count = Column(Integer)
    source_run_count = Column(Integer)
    baseline_confidence = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)


# stores each rolling live window and its calculated features
# live equivalent of historical windowed feature dataset
class LiveProcessWindow(Base):
    """Calculated live rolling-window feature and state snapshot."""

    __tablename__ = "live_process_window"

    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_id = Column(Integer, nullable=True)
    line_id = Column(Integer, nullable=True)
    production_run_id = Column(Integer, nullable=True)
    window_start = Column(DateTime)
    window_end = Column(DateTime)
    row_count = Column(Integer)
    valid_fraction = Column(Float)
    invalid_fraction = Column(Float)
    outlier_fraction = Column(Float)
    avg_pressure = Column(Float)
    avg_speed = Column(Float)
    avg_temp = Column(Float)
    avg_load = Column(Float)
    min_pressure = Column(Float)
    max_pressure = Column(Float)
    min_speed = Column(Float)
    max_speed = Column(Float)
    pressure_std = Column(Float)
    speed_std = Column(Float)
    temp_std = Column(Float)
    pressure_range = Column(Float)
    speed_range = Column(Float)
    temp_range = Column(Float)
    pressure_slope = Column(Float)
    speed_slope = Column(Float)
    temp_slope = Column(Float)
    pressure_per_rpm = Column(Float)
    temp_spread = Column(Float)
    load_per_pressure = Column(Float)
    candidate_state = Column(String)
    confirmed_state = Column(String, nullable=True)
    confirmation_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


# stores per-feature evaluation result for one live window
# UI uses this to show which features are normal/warning/critical
class LiveFeatureEvaluation(Base):
    """Per-feature live-vs-baseline evaluation output."""

    __tablename__ = "live_feature_evaluation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    live_process_window_id = Column(Integer, nullable=True)
    live_run_evaluation_id = Column(Integer, nullable=True)
    feature_name = Column(String)
    current_value = Column(Float)
    baseline_id = Column(Integer, nullable=True)
    baseline_mean = Column(Float, nullable=True)
    baseline_std = Column(Float, nullable=True)
    baseline_warning_low = Column(Float, nullable=True)
    baseline_warning_high = Column(Float, nullable=True)
    baseline_critical_low = Column(Float, nullable=True)
    baseline_critical_high = Column(Float, nullable=True)
    deviation_abs = Column(Float, nullable=True)
    deviation_pct = Column(Float, nullable=True)
    z_score = Column(Float, nullable=True)
    feature_status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


# top-level evaluation result for one live window
# main output for UI, reporting and alerts
class LiveRunEvaluation(Base):
    """Top-level evaluation outcome for one live process window."""

    __tablename__ = "live_run_evaluation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    live_process_window_id = Column(Integer, nullable=True)
    machine_id = Column(Integer, nullable=True)
    line_id = Column(Integer, nullable=True)
    production_run_id = Column(Integer, nullable=True)
    detected_state = Column(String)
    active_regime = Column(String, nullable=True)
    matched_profile_id = Column(Integer, nullable=True)
    baseline_id = Column(Integer, nullable=True)
    baseline_selection_method = Column(String, nullable=True)
    evaluation_status = Column(String)
    overall_status = Column(String, nullable=True)
    stability_status = Column(String, nullable=True)
    drift_score = Column(Float, nullable=True)
    anomaly_score = Column(Float, nullable=True)
    explanation_text = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# creates new tables if they don't exist yet
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
                "Features saved for window: %s → %s",
                features.get("window_start"),
                features.get("window_end"),
            )
        except Exception as exc:  # pragma: no cover - runtime DB safety
            session.rollback()
            logging.warning("Failed to save features: %s", exc)
        finally:
            session.close()

    # saves current rolling window features to LiveProcessWindow table
    # called every cycle after feature calculation and state detection
    def save_live_process_window(self, features: dict, state_info: dict):
        """Save one live rolling window row and return inserted object."""
        session = Session(self.engine)
        try:
            live_window = LiveProcessWindow(
                window_start=features["window_start"],
                window_end=features["window_end"],
                row_count=features.get("row_count", 0),
                valid_fraction=features.get("valid_fraction", 1.0),
                invalid_fraction=features.get("invalid_fraction", 0.0),
                outlier_fraction=features.get("outlier_fraction", 0.0),
                avg_pressure=features.get("pressure_mean"),
                avg_speed=features.get("screw_speed_mean"),
                avg_temp=features.get("temperature_mean"),
                avg_load=features.get("load_mean"),
                min_pressure=features.get("pressure_min"),
                max_pressure=features.get("pressure_max"),
                min_speed=features.get("screw_speed_min"),
                max_speed=features.get("screw_speed_max"),
                pressure_std=features.get("pressure_std"),
                speed_std=features.get("screw_speed_std"),
                temp_std=features.get("temperature_std"),
                pressure_range=features.get("pressure_range"),
                speed_range=features.get("screw_speed_range"),
                temp_range=features.get("temperature_range"),
                pressure_slope=features.get("pressure_trend"),
                speed_slope=features.get("screw_speed_trend"),
                temp_slope=features.get("temperature_trend"),
                pressure_per_rpm=features.get("pressure_per_rpm"),
                temp_spread=features.get("temp_spread"),
                load_per_pressure=features.get("load_per_pressure"),
                candidate_state=state_info.get("candidate_state"),
                confirmed_state=state_info.get("confirmed_state"),
                confirmation_count=state_info.get("confirmation_count", 0),
            )
            session.add(live_window)
            session.commit()
            session.refresh(live_window)
            logging.info(
                "LiveProcessWindow saved: id=%s state=%s",
                live_window.id,
                live_window.confirmed_state,
            )
            # return id is important -- evaluation tables link back to this
            return live_window
        except Exception as exc:  # pragma: no cover - runtime DB safety
            session.rollback()
            logging.warning("Failed to save LiveProcessWindow: %s", exc)
            return None
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
                "State saved: candidate=%s confirmed=%s",
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

