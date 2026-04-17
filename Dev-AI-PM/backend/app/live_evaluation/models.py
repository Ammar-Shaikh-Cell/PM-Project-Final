from sqlalchemy import Column, DateTime, ForeignKey, Index, Numeric, String, Text, Uuid
from sqlalchemy.sql import func

from app.models.base import Base


class MachineSensorRaw(Base):
    __tablename__ = "machine_sensor_raw"

    machine_id = Column(ForeignKey("machine.id", ondelete="CASCADE"), nullable=False, index=True)
    sensor_id = Column(ForeignKey("sensor.id", ondelete="CASCADE"), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    value = Column(Numeric, nullable=True)

    __table_args__ = (
        Index("ix_machine_sensor_raw_machine_timestamp", "machine_id", "timestamp"),
    )


class BaselineRegistry(Base):
    __tablename__ = "baseline_registry"

    machine_id = Column(ForeignKey("machine.id", ondelete="CASCADE"), nullable=False, index=True)
    profile_id = Column(Uuid(as_uuid=True), ForeignKey("profiles.id", ondelete="SET NULL"), nullable=True, index=True)
    regime = Column(String(16), nullable=False, index=True)
    sensor_id = Column(ForeignKey("sensor.id", ondelete="CASCADE"), nullable=False, index=True)
    baseline_mean = Column(Numeric, nullable=True)
    baseline_std = Column(Numeric, nullable=True)

    __table_args__ = (
        Index("ix_baseline_registry_machine_regime_profile", "machine_id", "regime", "profile_id"),
    )


class LiveProcessWindow(Base):
    __tablename__ = "live_process_window"

    machine_id = Column(ForeignKey("machine.id", ondelete="CASCADE"), nullable=False, index=True)
    window_start = Column(DateTime(timezone=True), nullable=False)
    window_end = Column(DateTime(timezone=True), nullable=False, index=True)
    regime = Column(String(16), nullable=True)
    profile_id = Column(Uuid(as_uuid=True), ForeignKey("profiles.id", ondelete="SET NULL"), nullable=True, index=True)
    state = Column(String(32), nullable=False, index=True)

    __table_args__ = (
        Index("ix_live_process_window_machine_window_end", "machine_id", "window_end"),
    )


class LiveRunEvaluation(Base):
    __tablename__ = "live_run_evaluation"

    window_id = Column(Uuid(as_uuid=True), ForeignKey("live_process_window.id", ondelete="CASCADE"), nullable=False, index=True)
    overall_status = Column(String(32), nullable=False)
    stability_status = Column(String(32), nullable=False)
    drift_score = Column(Numeric, nullable=True)
    anomaly_score = Column(Numeric, nullable=True)
    explanation_text = Column(Text, nullable=True)
    baseline_source = Column(String(64), nullable=True)


class LiveFeatureEvaluation(Base):
    __tablename__ = "live_feature_evaluation"

    window_id = Column(Uuid(as_uuid=True), ForeignKey("live_process_window.id", ondelete="CASCADE"), nullable=False, index=True)
    sensor_id = Column(ForeignKey("sensor.id", ondelete="CASCADE"), nullable=False, index=True)
    z_score = Column(Numeric, nullable=True)
    pct_deviation = Column(Numeric, nullable=True)
    severity = Column(String(16), nullable=False)


class EvaluationConfig(Base):
    __tablename__ = "evaluation_config"

    machine_id = Column(ForeignKey("machine.id", ondelete="CASCADE"), nullable=False, index=True)
    config_key = Column(String(128), nullable=False)
    config_value = Column(Text, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_evaluation_config_machine_key_unique", "machine_id", "config_key", unique=True),
    )
