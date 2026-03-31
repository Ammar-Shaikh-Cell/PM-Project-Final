from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    Column,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Float,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship

from app.models.base import Base
from app.models.machine import Machine


class Profile(Base):
    """
    Evaluation profile for a given (machine, material).

    - One active profile per (machine, material_id) should be enforced at the service layer.
    - If machine_id is NULL, the profile acts as a material default profile.
    """

    __tablename__ = "profiles"
    __mapper_args__ = {'exclude_properties': ['updated_at']}  # Exclude updated_at since it doesn't exist in DB yet

    # Override Base.id to be explicit UUID for clarity in migrations
    id = Column(PG_UUID(as_uuid=True), primary_key=True)

    # Machine-specific profile; nullable for material default
    machine_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("machine.id"),
        nullable=True,
        index=True,
    )

    # Simple material identifier (string key, e.g. "Material 1")
    material_id = Column(String(100), nullable=False, index=True)

    # Profile status and versioning
    is_active = Column(Boolean, nullable=False, default=True)
    version = Column(String(50), nullable=True)
    
    # Baseline learning lifecycle flags
    baseline_learning = Column(Boolean, nullable=False, default=False)
    baseline_ready = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    # Note: updated_at column does not exist in database - excluded via __mapper_args__
    # When adding via migration, remove __mapper_args__ exclusion and add column definition

    # Relationships
    machine = relationship(Machine, backref="profiles")
    state_thresholds = relationship(
        "ProfileStateThresholds", back_populates="profile", cascade="all, delete-orphan"
    )
    baseline_stats = relationship(
        "ProfileBaselineStats", back_populates="profile", cascade="all, delete-orphan"
    )
    baseline_samples = relationship(
        "ProfileBaselineSample", back_populates="profile", cascade="all, delete-orphan"
    )
    scoring_bands = relationship(
        "ProfileScoringBand", back_populates="profile", cascade="all, delete-orphan"
    )
    message_templates = relationship(
        "ProfileMessageTemplate", back_populates="profile", cascade="all, delete-orphan"
    )


class ProfileStateThresholds(Base):
    """
    State detection thresholds bound to a specific profile.

    These mirror the core thresholds currently in MachineStateThresholds but are
    organized per (machine, material) profile.
    """

    __tablename__ = "profile_state_thresholds"

    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    profile_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Core thresholds
    rpm_on = Column(Float, nullable=False)  # RPM_ON
    rpm_prod = Column(Float, nullable=False)  # RPM_PROD
    p_on = Column(Float, nullable=False)  # P_ON
    p_prod = Column(Float, nullable=False)  # P_PROD
    t_min_active = Column(Float, nullable=False)  # T_MIN_ACTIVE

    # Temperature rate thresholds
    heating_rate = Column(Float, nullable=False)  # HEATING_RATE
    cooling_rate = Column(Float, nullable=False)  # COOLING_RATE

    # Hysteresis timers (seconds)
    enter_production_sec = Column(Float, nullable=False)
    exit_production_sec = Column(Float, nullable=False)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    profile = relationship("Profile", back_populates="state_thresholds")


class ProfileBaselineStats(Base):
    """
    Baseline statistics for each metric within a profile.
    """

    __tablename__ = "profile_baseline_stats"
    __mapper_args__ = {'exclude_properties': ['created_at', 'updated_at']}  # Exclude columns that don't exist in DB

    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    profile_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    metric_name = Column(String(100), nullable=False)

    baseline_mean = Column(Float, nullable=True)
    baseline_std = Column(Float, nullable=True)
    p05 = Column(Float, nullable=True)
    p95 = Column(Float, nullable=True)
    sample_count = Column(Float, nullable=True)
    last_updated = Column(DateTime(timezone=True), default=datetime.utcnow)

    profile = relationship("Profile", back_populates="baseline_stats")


class ProfileBaselineSample(Base):
    """
    Temporary storage for baseline learning samples.
    
    Samples are collected during baseline_learning mode and used to compute
    baseline statistics when finalizing the baseline.
    """

    __tablename__ = "profile_baseline_samples"

    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    profile_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    metric_name = Column(String(100), nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    profile = relationship("Profile", back_populates="baseline_samples")


class ProfileScoringBand(Base):
    """
    Scoring bands for mapping metric deviations to traffic light colors.

    mode:
      - ABS: absolute thresholds
      - REL: relative to baseline (e.g. z-score or percentage)
    """

    __tablename__ = "profile_scoring_bands"

    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    profile_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    metric_name = Column(String(100), nullable=False)

    # "ABS" | "REL"
    mode = Column(String(10), nullable=False)

    # Thresholds for green/orange/red decision
    green_limit = Column(Float, nullable=True)
    orange_limit = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    profile = relationship("Profile", back_populates="scoring_bands")


class ProfileMessageTemplate(Base):
    """
    Text messages shown for each metric + severity per profile.
    """

    __tablename__ = "profile_message_templates"

    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    profile_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    metric_name = Column(String(100), nullable=False)
    severity = Column(String(10), nullable=False)  # GREEN | ORANGE | RED
    text = Column(Text, nullable=False)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    profile = relationship("Profile", back_populates="message_templates")

