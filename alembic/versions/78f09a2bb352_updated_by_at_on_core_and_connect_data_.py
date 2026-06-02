"""updated_by and updated_at audit on core and connect data items

Revision ID: 78f09a2bb352
Revises: 62b00942dc72
Create Date: 2026-06-02 09:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '78f09a2bb352'
down_revision: Union[str, None] = '62b00942dc72'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CoreDataItem already has updated_at; only updated_by is missing.
    op.add_column(
        'core_data_items',
        sa.Column('updated_by', sa.String(length=36), nullable=True),
    )
    op.create_foreign_key(
        'fk_core_data_items_updated_by_users',
        'core_data_items', 'users',
        ['updated_by'], ['id'],
    )

    # ConnectDataItem has neither.
    op.add_column(
        'connect_data_items',
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        'connect_data_items',
        sa.Column('updated_by', sa.String(length=36), nullable=True),
    )
    op.create_foreign_key(
        'fk_connect_data_items_updated_by_users',
        'connect_data_items', 'users',
        ['updated_by'], ['id'],
    )

    # Backfill: for existing rows, "last updated" == creation. This way the
    # frontend can hide the "Updated by …" line until the row is actually
    # edited (by comparing updated_at and created_at).
    op.execute("UPDATE core_data_items SET updated_by = created_by WHERE updated_by IS NULL")
    op.execute("UPDATE connect_data_items SET updated_at = created_at, updated_by = created_by WHERE updated_at IS NULL")


def downgrade() -> None:
    op.drop_constraint('fk_connect_data_items_updated_by_users', 'connect_data_items', type_='foreignkey')
    op.drop_column('connect_data_items', 'updated_by')
    op.drop_column('connect_data_items', 'updated_at')
    op.drop_constraint('fk_core_data_items_updated_by_users', 'core_data_items', type_='foreignkey')
    op.drop_column('core_data_items', 'updated_by')
