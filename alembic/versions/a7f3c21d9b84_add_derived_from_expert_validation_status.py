"""add DERIVED_FROM_EXPERT to validationstatus

Translations re-derived from an expert correction in a sibling language get
their own status so they are distinguishable from never-reviewed machine
output. See app/tasks/propagate_expert.py.

Postgres allows ALTER TYPE ... ADD VALUE inside a transaction since 12, as
long as the new value is not *used* in the same transaction. This migration
only adds it, so the default transactional run is fine. IF NOT EXISTS keeps
it idempotent across re-runs.

Revision ID: a7f3c21d9b84
Revises: 00e6db3c361c
Create Date: 2026-11-05
"""
from alembic import op

revision = "a7f3c21d9b84"
down_revision = "00e6db3c361c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE validationstatus ADD VALUE IF NOT EXISTS 'DERIVED_FROM_EXPERT'")


def downgrade() -> None:
    # Postgres cannot drop a value from an enum type. Reverting means
    # recreating the type, which requires rewriting every dependent column —
    # and any row already carrying the value would have to be reassigned
    # first. Collapse those rows back to MACHINE_GENERATED (the state they
    # would have been in) and leave the unused label on the type.
    op.execute(
        "UPDATE core_data_translations SET validation_status = 'MACHINE_GENERATED' "
        "WHERE validation_status = 'DERIVED_FROM_EXPERT'"
    )
