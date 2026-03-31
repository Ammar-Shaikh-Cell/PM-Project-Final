"""Add profiles and related evaluation configuration tables

Revision ID: 0006_add_profiles_and_evaluation_config
Revises: 0005_add_machine_state
Create Date: 2026-02-09
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0006_add_profiles_and_evaluation_config"
down_revision: Union[str, None] = "0005_add_machine_state"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create profiles table
    op.create_table(
        "profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "machine_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("machine.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("material_id", sa.String(length=100), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("version", sa.String(length=50), nullable=True),
        sa.Column("baseline_learning", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("baseline_ready", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_profiles_machine_id_material_id",
        "profiles",
        ["machine_id", "material_id"],
        unique=False,
    )
    op.create_index(
        "ix_profiles_material_id",
        "profiles",
        ["material_id"],
        unique=False,
    )

    # Create profile_state_thresholds table
    op.create_table(
        "profile_state_thresholds",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "profile_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rpm_on", sa.Float(), nullable=False),
        sa.Column("rpm_prod", sa.Float(), nullable=False),
        sa.Column("p_on", sa.Float(), nullable=False),
        sa.Column("p_prod", sa.Float(), nullable=False),
        sa.Column("t_min_active", sa.Float(), nullable=False),
        sa.Column("heating_rate", sa.Float(), nullable=False),
        sa.Column("cooling_rate", sa.Float(), nullable=False),
        sa.Column("enter_production_sec", sa.Float(), nullable=False),
        sa.Column("exit_production_sec", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_profile_state_thresholds_profile_id",
        "profile_state_thresholds",
        ["profile_id"],
        unique=False,
    )

    # Create profile_baseline_stats table
    op.create_table(
        "profile_baseline_stats",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "profile_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_name", sa.String(length=100), nullable=False),
        sa.Column("baseline_mean", sa.Float(), nullable=True),
        sa.Column("baseline_std", sa.Float(), nullable=True),
        sa.Column("p05", sa.Float(), nullable=True),
        sa.Column("p95", sa.Float(), nullable=True),
        sa.Column("sample_count", sa.Float(), nullable=True),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_profile_baseline_stats_profile_metric",
        "profile_baseline_stats",
        ["profile_id", "metric_name"],
        unique=False,
    )

    # Create profile_baseline_samples table (temporary storage during learning)
    op.create_table(
        "profile_baseline_samples",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "profile_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_name", sa.String(length=100), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_profile_baseline_samples_profile_metric",
        "profile_baseline_samples",
        ["profile_id", "metric_name"],
        unique=False,
    )
    op.create_index(
        "ix_profile_baseline_samples_timestamp",
        "profile_baseline_samples",
        ["timestamp"],
        unique=False,
    )

    # Create profile_scoring_bands table
    op.create_table(
        "profile_scoring_bands",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "profile_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_name", sa.String(length=100), nullable=False),
        sa.Column("mode", sa.String(length=10), nullable=False),
        sa.Column("green_limit", sa.Float(), nullable=True),
        sa.Column("orange_limit", sa.Float(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_profile_scoring_bands_profile_metric",
        "profile_scoring_bands",
        ["profile_id", "metric_name"],
        unique=False,
    )

    # Create profile_message_templates table
    op.create_table(
        "profile_message_templates",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "profile_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_name", sa.String(length=100), nullable=False),
        sa.Column("severity", sa.String(length=10), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_profile_message_templates_profile_metric_severity",
        "profile_message_templates",
        ["profile_id", "metric_name", "severity"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_profile_message_templates_profile_metric_severity", table_name="profile_message_templates")
    op.drop_table("profile_message_templates")

    op.drop_index("ix_profile_scoring_bands_profile_metric", table_name="profile_scoring_bands")
    op.drop_table("profile_scoring_bands")

    op.drop_index("ix_profile_baseline_samples_timestamp", table_name="profile_baseline_samples")
    op.drop_index("ix_profile_baseline_samples_profile_metric", table_name="profile_baseline_samples")
    op.drop_table("profile_baseline_samples")

    op.drop_index("ix_profile_baseline_stats_profile_metric", table_name="profile_baseline_stats")
    op.drop_table("profile_baseline_stats")

    op.drop_index("ix_profile_state_thresholds_profile_id", table_name="profile_state_thresholds")
    op.drop_table("profile_state_thresholds")

    op.drop_index("ix_profiles_material_id", table_name="profiles")
    op.drop_index("ix_profiles_machine_id_material_id", table_name="profiles")
    op.drop_table("profiles")

