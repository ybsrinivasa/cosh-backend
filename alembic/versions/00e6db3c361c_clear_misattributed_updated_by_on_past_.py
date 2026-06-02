"""clear misattributed updated_by on past-edited core items

When the previous migration backfilled updated_by = created_by on every
row, that was correct ONLY for rows that had never actually been edited.
For rows where the system DID record an updated_at later than created_at
(7,000+ Core items on production), the creator is almost certainly not
the editor — they just happened to be the only user-id we had on file.

To avoid showing misleading "Updated by Alice" labels on rows that some
other user actually edited, null out updated_by on every row where the
edit timestamp is meaningfully later than the creation timestamp.
The frontend will then render "Updated · <date>" with no name —
honest about what we can't recover.

Connect data items aren't affected — their updated_at column was added
in the same prior migration, so every row currently has
updated_at == created_at (no past edits are visible).

Revision ID: 00e6db3c361c
Revises: 78f09a2bb352
Create Date: 2026-06-02 20:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = '00e6db3c361c'
down_revision: Union[str, None] = '78f09a2bb352'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 2-second window absorbs the small skew between SQLAlchemy's `default`
    # and `onupdate` callbacks at INSERT time.
    op.execute("""
        UPDATE core_data_items
           SET updated_by = NULL
         WHERE updated_at > created_at + INTERVAL '2 seconds'
    """)


def downgrade() -> None:
    # Intentional no-op. The previous migration backfilled updated_by =
    # created_by indiscriminately; restoring that would just reintroduce
    # the misattribution this migration removed.
    pass
