"""Add evaluation_config table and seed baseline keys

Revision ID: 0009_add_evaluation_config
Revises: 0008_add_profile_pressure_config
Create Date: 2026-04-13
"""

from typing import Sequence, Union
from uuid import uuid4

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0009_add_evaluation_config"
down_revision: Union[str, None] = "0008_add_profile_pressure_config"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "evaluation_config",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "machine_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("machine.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("config_key", sa.String(length=128), nullable=False),
        sa.Column("config_value", sa.Text(), nullable=False),
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
    op.create_index("ix_evaluation_config_machine_id", "evaluation_config", ["machine_id"], unique=False)
    op.create_index(
        "ix_evaluation_config_machine_key_unique",
        "evaluation_config",
        ["machine_id", "config_key"],
        unique=True,
    )

    bind = op.get_bind()
    machine_rows = bind.execute(sa.text("SELECT id FROM machine")).fetchall()
    default_items = [
        ("severity_warning_threshold", "2.0"),
        ("severity_critical_threshold", "3.0"),
        ("drift_weight", "0.5"),
        ("anomaly_weight", "0.5"),
        ("stability_threshold", "0.7"),
    ]

    if machine_rows:
        insert_stmt = sa.text(
            """
            INSERT INTO evaluation_config (id, machine_id, config_key, config_value, created_at, updated_at)
            VALUES (:id, :machine_id, :config_key, :config_value, NOW(), NOW())
            """
        )
        for row in machine_rows:
            machine_id = row[0]
            for config_key, config_value in default_items:
                bind.execute(
                    insert_stmt,
                    {
                        "id": uuid4(),
                        "machine_id": machine_id,
                        "config_key": config_key,
                        "config_value": config_value,
                    },
                )


def downgrade() -> None:
    op.drop_index("ix_evaluation_config_machine_key_unique", table_name="evaluation_config")
    op.drop_index("ix_evaluation_config_machine_id", table_name="evaluation_config")
    op.drop_table("evaluation_config")
