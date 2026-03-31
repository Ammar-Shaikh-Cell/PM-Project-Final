"""Add per-profile melt pressure configuration

Revision ID: 0008_add_profile_pressure_config
Revises: 0007_add_email_recipients
Create Date: 2026-03-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0008_add_profile_pressure_config"
down_revision: Union[str, None] = "0007_add_email_recipients"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "profile_pressure_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "profile_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("batch_id", sa.String(length=100), nullable=True),
        sa.Column("cool_from", sa.Float(), nullable=False, server_default=sa.text("330")),
        sa.Column("cool_to", sa.Float(), nullable=False, server_default=sa.text("360")),
        sa.Column("medium_from", sa.Float(), nullable=False, server_default=sa.text("360")),
        sa.Column("medium_to", sa.Float(), nullable=False, server_default=sa.text("380")),
        sa.Column("hot_from", sa.Float(), nullable=False, server_default=sa.text("380")),
        sa.Column("hot_to", sa.Float(), nullable=False, server_default=sa.text("395")),
        sa.Column("critical_from", sa.Float(), nullable=False, server_default=sa.text("395")),
        sa.Column("critical_to", sa.Float(), nullable=False, server_default=sa.text("410")),
        sa.Column("warning_threshold", sa.Float(), nullable=False, server_default=sa.text("380")),
        sa.Column("critical_warning_threshold", sa.Float(), nullable=False, server_default=sa.text("395")),
        sa.Column("low_pressure_warning_threshold", sa.Float(), nullable=False, server_default=sa.text("340")),
        sa.Column("send_email_on_warning", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("send_email_on_critical", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("send_email_on_production_start", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("send_email_on_production_stop", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
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
        "ix_profile_pressure_configs_profile_id",
        "profile_pressure_configs",
        ["profile_id"],
        unique=False,
    )
    op.create_index(
        "ix_profile_pressure_configs_profile_batch_active",
        "profile_pressure_configs",
        ["profile_id", "batch_id", "is_active"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_profile_pressure_configs_profile_batch_active", table_name="profile_pressure_configs")
    op.drop_index("ix_profile_pressure_configs_profile_id", table_name="profile_pressure_configs")
    op.drop_table("profile_pressure_configs")

