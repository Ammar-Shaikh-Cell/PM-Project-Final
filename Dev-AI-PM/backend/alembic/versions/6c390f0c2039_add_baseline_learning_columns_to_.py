"""add_baseline_learning_columns_to_profiles

Revision ID: 6c390f0c2039
Revises: 0006_add_profiles_and_evaluation_config
Create Date: 2026-02-09 11:25:33.543796

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = '6c390f0c2039'
down_revision: Union[str, None] = '0006_add_profiles_and_evaluation_config'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Check if columns exist before adding them
    conn = op.get_bind()
    inspector = inspect(conn)
    
    # Check if profiles table exists
    if 'profiles' in inspector.get_table_names():
        columns = [col['name'] for col in inspector.get_columns('profiles')]
        
        if 'baseline_learning' not in columns:
            op.add_column('profiles', sa.Column('baseline_learning', sa.Boolean(), nullable=False, server_default=sa.text('false')))
        
        if 'baseline_ready' not in columns:
            op.add_column('profiles', sa.Column('baseline_ready', sa.Boolean(), nullable=False, server_default=sa.text('false')))


def downgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    
    if 'profiles' in inspector.get_table_names():
        columns = [col['name'] for col in inspector.get_columns('profiles')]
        
        if 'baseline_ready' in columns:
            op.drop_column('profiles', 'baseline_ready')
        
        if 'baseline_learning' in columns:
            op.drop_column('profiles', 'baseline_learning')
