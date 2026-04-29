"""hypergraph_connect_positions

Revision ID: 3b6c4430eeda
Revises: 0f2238754185
Create Date: 2026-04-29 10:48:39.953608

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3b6c4430eeda'
down_revision: Union[str, None] = '0f2238754185'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


nodetype_enum = sa.Enum('CORE', 'CONNECT', name='nodetype')


def upgrade() -> None:
    """Upgrade schema."""
    nodetype_enum.create(op.get_bind(), checkfirst=True)
    op.add_column('connect_data_positions', sa.Column('connect_data_item_ref_id', sa.String(length=36), nullable=True))
    op.alter_column('connect_data_positions', 'core_data_item_id',
               existing_type=sa.VARCHAR(length=36),
               nullable=True)
    op.create_foreign_key('fk_cdp_connect_data_item_ref', 'connect_data_positions', 'connect_data_items', ['connect_data_item_ref_id'], ['id'])
    op.add_column('connect_schema_positions', sa.Column('node_type', nodetype_enum, server_default='CORE', nullable=False))
    op.add_column('connect_schema_positions', sa.Column('connect_ref_id', sa.String(length=36), nullable=True))
    op.alter_column('connect_schema_positions', 'core_id',
               existing_type=sa.VARCHAR(length=36),
               nullable=True)
    op.create_foreign_key('fk_csp_connect_ref', 'connect_schema_positions', 'connects', ['connect_ref_id'], ['id'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('fk_csp_connect_ref', 'connect_schema_positions', type_='foreignkey')
    op.alter_column('connect_schema_positions', 'core_id',
               existing_type=sa.VARCHAR(length=36),
               nullable=False)
    op.drop_column('connect_schema_positions', 'connect_ref_id')
    op.drop_column('connect_schema_positions', 'node_type')
    op.drop_constraint('fk_cdp_connect_data_item_ref', 'connect_data_positions', type_='foreignkey')
    op.alter_column('connect_data_positions', 'core_data_item_id',
               existing_type=sa.VARCHAR(length=36),
               nullable=False)
    op.drop_column('connect_data_positions', 'connect_data_item_ref_id')
    nodetype_enum.drop(op.get_bind(), checkfirst=True)
