"""
Celery task: push an expert's correction out to the other languages.

Scheduled with a countdown from the handlers in routers/cores.py that write an
EXPERT_VALIDATED translation. Debounced without any extra state — see
`_is_stale` below.

Writes DERIVED_FROM_EXPERT rows, never touches EXPERT_VALIDATED, and emits the
sync change-log events so the result actually reaches RootsTalk (the omission
fixed in ed3d05d — a translation write with no change row is invisible to
INCREMENTAL sync forever).
"""
import logging

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.celery_app import celery_app
from app.services.expert_propagation import (
    PropagationUnavailableError,
    propagate_from_expert,
)

logger = logging.getLogger(__name__)

# Delay between the curator's save and the propagation run. Long enough that
# a burst of edits on one row collapses to a single run, short enough that the
# other languages catch up while the curator is still in the Core.
PROPAGATE_COUNTDOWN_SECONDS = 30


def _get_sync_engine():
    import os

    from dotenv import load_dotenv

    load_dotenv()
    return create_engine(os.getenv("DATABASE_URL_SYNC"))


def schedule_propagation(item_id: str, source_lang: str, validated_at) -> None:
    """Queue a propagation run for one item.

    `validated_at` is the source row's timestamp as it stood when this was
    scheduled; it is the debounce token (see `_is_stale`). Safe to call on
    every expert save.
    """
    propagate_expert_correction.apply_async(
        args=[item_id, source_lang, validated_at.isoformat() if validated_at else None],
        countdown=PROPAGATE_COUNTDOWN_SECONDS,
    )


def _is_stale(current_validated_at, token: str | None) -> bool:
    """Debounce, with no Redis key and no bookkeeping table.

    Each scheduled run carries the source row's `validated_at` as it was at
    schedule time. If the row has been saved again since, the stored timestamp
    no longer matches our token, so this run is superseded — the later run
    carries the newer token and will do the work. Exactly one run per settled
    edit, whatever order they fire in.
    """
    if token is None:
        return False
    if current_validated_at is None:
        return True
    return current_validated_at.isoformat() != token


@celery_app.task(
    name="app.tasks.propagate_expert.propagate_expert_correction",
    bind=True,
    max_retries=2,
)
def propagate_expert_correction(
    self, item_id: str, source_lang: str, validated_at_token: str | None = None
):
    from app.models.models import (
        ChangeType,
        Core,
        CoreDataItem,
        CoreDataTranslation,
        CoreLanguageConfig,
        CoreProductTag,
        EntityType,
        SyncChangeLog,
        ValidationStatus,
    )

    engine = _get_sync_engine()

    with Session(engine) as session:
        item = session.execute(
            select(CoreDataItem).where(CoreDataItem.id == item_id)
        ).scalar_one_or_none()
        if not item:
            logger.warning(f"[propagate] item {item_id} not found")
            return

        source = session.execute(
            select(CoreDataTranslation).where(
                CoreDataTranslation.item_id == item_id,
                CoreDataTranslation.language_code == source_lang,
            )
        ).scalar_one_or_none()
        if not source:
            logger.warning(f"[propagate] no {source_lang} row for item {item_id}")
            return

        # The anchor must still be an expert value. If a later machine run
        # overwrote it, there is nothing authoritative to propagate from.
        if source.validation_status != ValidationStatus.EXPERT_VALIDATED:
            logger.info(
                f"[propagate] {source_lang} on {item_id} is no longer "
                f"EXPERT_VALIDATED — skipping"
            )
            return

        if _is_stale(source.validated_at, validated_at_token):
            logger.info(
                f"[propagate] superseded by a newer edit on {item_id} "
                f"({source_lang}) — skipping"
            )
            return

        core = session.execute(
            select(Core).where(Core.id == item.core_id)
        ).scalar_one_or_none()

        # Only languages configured on the Core, excluding English and the
        # anchor itself. A row absent here is simply not yet translated; the
        # ordinary translate_item path will fill it.
        configured = {
            c.language_code
            for c in session.execute(
                select(CoreLanguageConfig).where(
                    CoreLanguageConfig.core_id == item.core_id
                )
            ).scalars()
        }
        targets = []
        rows_by_lang = {}
        for row in session.execute(
            select(CoreDataTranslation).where(
                CoreDataTranslation.item_id == item_id
            )
        ).scalars():
            if row.language_code in (source_lang, "en"):
                continue
            if row.language_code not in configured:
                continue
            # A curator's own work in another language is final.
            if row.validation_status == ValidationStatus.EXPERT_VALIDATED:
                continue
            targets.append((row.language_code, row.translated_value))
            rows_by_lang[row.language_code] = row

        if not targets:
            logger.info(f"[propagate] nothing overwritable on {item_id}")
            return

        try:
            result = propagate_from_expert(
                core.name if core else None,
                core.description if core else None,
                item.english_value,
                source_lang,
                source.translated_value,
                targets,
            )
        except PropagationUnavailableError as e:
            logger.error(f"[propagate] ABORTING item {item_id} — {e}")
            raise

        if not result:
            return

        if not result.get("correction_is_semantic"):
            logger.info(
                f"[propagate] {item.english_value!r}: correction is "
                f"{source_lang}-internal — other languages untouched"
            )
            return

        changed = []
        for entry in result.get("languages", []):
            lang = entry.get("lang")
            row = rows_by_lang.get(lang)
            if row is None or entry.get("verdict") != "wrong":
                continue
            new_value = (entry.get("corrected") or "").strip()
            if not new_value or new_value == row.translated_value:
                continue
            row.translated_value = new_value
            row.validation_status = ValidationStatus.DERIVED_FROM_EXPERT
            # Not a human validation — leave validated_by/validated_at alone so
            # they keep meaning "an expert signed this off".
            changed.append(lang)

        if not changed:
            logger.info(
                f"[propagate] {item.english_value!r}: other languages already "
                f"consistent with the {source_lang} correction"
            )
            return

        # BL-C-07: without this the rewritten values never reach RootsTalk.
        product_ids = session.execute(
            select(CoreProductTag.product_id).where(
                CoreProductTag.core_id == item.core_id
            )
        ).scalars().all()
        for pid in product_ids:
            session.add(
                SyncChangeLog(
                    product_id=pid,
                    entity_type=EntityType.TRANSLATION,
                    entity_id=item_id,
                    change_type=ChangeType.TRANSLATION_UPDATED,
                )
            )

        session.commit()
        logger.info(
            f"[propagate] {item.english_value!r}: rewrote {', '.join(sorted(changed))} "
            f"from the {source_lang} correction "
            f"(sync events for {len(product_ids)} product(s))"
        )
