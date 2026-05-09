"""Add position_label to connect_schema_positions

Revision ID: 62b00942dc72
Revises: 6eba5fd99afa
Create Date: 2026-05-09 09:30:24.057855

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '62b00942dc72'
down_revision: Union[str, None] = '6eba5fd99afa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "connect_schema_positions",
        sa.Column("position_label", sa.String(length=200), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("connect_schema_positions", "position_label")
