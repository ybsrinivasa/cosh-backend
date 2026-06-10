"""
Celery translation tasks — all async, never block the API.
Uses synchronous SQLAlchemy since Celery workers are not async.

Each task fetches the parent Core's language_mode and routes:
  - TRANSLATION (or NULL — the default for legacy Cores) → IndicTrans2
  - TRANSLITERATION → IndicXlit

Both engines write into core_data_translations; the downstream
consumer (sync, UI) reads `translated_value` without caring which
engine produced it.
"""
import logging
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.celery_app import celery_app
from app.services.translation_service import translate_text
from app.services.transliteration_service import transliterate_text
from app.services.claude_translation_service import claude_translate

logger = logging.getLogger(__name__)

# Number of new/changed translation rows between intermediate commits in
# the bulk tasks. Keep small so progress is visible live in the DB and so
# a worker crash mid-task only loses ~COMMIT_EVERY rows of work.
# 50 rows × ~1.5 s/translation = ~75 s between commits — comfortable for
# observability and well under the broker visibility_timeout.
COMMIT_EVERY = 50


def _get_sync_engine():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    url = os.getenv("DATABASE_URL_SYNC")
    return create_engine(url)


def _process_text(
    text: str,
    source_lang: str,
    target_lang: str,
    mode,
    core_name: str | None = None,
    core_description: str | None = None,
) -> str | None:
    """Route to the right engine based on the Core's language_mode.

    `mode` is a LanguageMode enum value or None. None defaults to
    TRANSLATION so existing Cores keep their current behaviour.

    TRANSLATION mode:
      Claude (domain-aware, sees core_name + core_description) → IndicTrans2 fallback
      → Google Translate fallback. Most calls land on Claude.

    TRANSLITERATION mode:
      IndicXlit only — sound-preserving brand/chemical names, no benefit
      from LLM context.
    """
    from app.models.models import LanguageMode
    if mode == LanguageMode.TRANSLITERATION:
        return transliterate_text(text, source_lang, target_lang)
    # TRANSLATION (or NULL): try Claude first with Core context.
    out = claude_translate(text, source_lang, target_lang, core_name, core_description)
    if out:
        return out
    # Fallback: original IndicTrans2/Google chain.
    return translate_text(text, source_lang, target_lang)


def _get_core_context(session: Session, core_id: str):
    """Load (mode, name, description) for the parent Core in one query.
    Returns (None, None, None) if not found (caller treats mode=None as TRANSLATION)."""
    from app.models.models import Core
    core = session.execute(select(Core).where(Core.id == core_id)).scalar_one_or_none()
    if not core:
        return None, None, None
    return core.language_mode, core.name, core.description


# Kept for backwards compat where only mode is needed.
def _get_core_mode(session: Session, core_id: str):
    mode, _, _ = _get_core_context(session, core_id)
    return mode


@celery_app.task(name="app.tasks.translation.translate_item", bind=True, max_retries=3)
def translate_item(self, item_id: str, english_value: str, target_langs: list[str]):
    """
    Process one Core Data Item into all configured languages.
    Triggered after item creation or English value edit.
    Only overwrites MACHINE_GENERATED rows — never EXPERT_VALIDATED.

    Honours the parent Core's language_mode: TRANSLITERATION cores call
    IndicXlit; everything else calls IndicTrans2 (with Google fallback).
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus

    engine = _get_sync_engine()

    with Session(engine) as session:
        item_row = session.execute(
            select(CoreDataItem).where(CoreDataItem.id == item_id)
        ).scalar_one_or_none()
        if not item_row:
            logger.warning(f"translate_item: item {item_id} not found")
            return
        mode, core_name, core_description = _get_core_context(session, item_row.core_id)

        for lang in target_langs:
            if lang == "en":
                continue

            existing = session.execute(
                select(CoreDataTranslation).where(
                    CoreDataTranslation.item_id == item_id,
                    CoreDataTranslation.language_code == lang,
                )
            ).scalar_one_or_none()

            if existing and existing.validation_status == ValidationStatus.EXPERT_VALIDATED:
                logger.info(f"Skipping EXPERT_VALIDATED translation [{lang}] for item {item_id}")
                continue

            processed = _process_text(english_value, "en", lang, mode, core_name, core_description)
            if not processed:
                continue

            if existing:
                existing.translated_value = processed
                existing.validation_status = ValidationStatus.MACHINE_GENERATED
            else:
                session.add(CoreDataTranslation(
                    item_id=item_id,
                    language_code=lang,
                    translated_value=processed,
                    validation_status=ValidationStatus.MACHINE_GENERATED,
                ))

        # BL-C-07: record TRANSLATION_UPDATED in sync_change_log
        from app.models.models import CoreProductTag, SyncChangeLog, EntityType, ChangeType
        product_ids = session.execute(
            select(CoreProductTag.product_id).where(CoreProductTag.core_id == item_row.core_id)
        ).scalars().all()
        for pid in product_ids:
            session.add(SyncChangeLog(
                product_id=pid,
                entity_type=EntityType.TRANSLATION,
                entity_id=item_id,
                change_type=ChangeType.TRANSLATION_UPDATED,
            ))

        session.commit()
    logger.info(f"Translation/transliteration complete for item {item_id} (mode={mode})")


@celery_app.task(name="app.tasks.translation.retranslate_core", bind=True, max_retries=2)
def retranslate_core(
    self,
    core_id: str,
    target_langs: list[str],
    overwrite_expert: bool = False,
    keywords: list[str] | None = None,
):
    """
    Re-process items in a Core.

    overwrite_expert=False: skip EXPERT_VALIDATED (safe default).
    overwrite_expert=True: overwrite everything (requires Designer confirmation).

    keywords: optional list of substrings. When non-empty, only items whose
    english_value contains any of these substrings (case-insensitive) are
    processed. Lets us cheaply fix terminology after a TERM_HINTS update —
    e.g., add "Aphid, Mite, Mosaic" and only ~150 items get re-translated
    instead of all 2,237.

    Commits every COMMIT_EVERY successful rows so progress is visible
    live and a worker crash mid-task only loses ~COMMIT_EVERY rows.
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus, StatusEnum
    from sqlalchemy import or_, func as safunc

    engine = _get_sync_engine()

    with Session(engine) as session:
        mode, core_name, core_description = _get_core_context(session, core_id)
        # Fetch as tuples — no ORM objects to worry about expiring on commit.
        query = select(CoreDataItem.id, CoreDataItem.english_value).where(
            CoreDataItem.core_id == core_id,
            CoreDataItem.status == StatusEnum.ACTIVE,
        )
        if keywords:
            # Case-insensitive substring match against english_value.
            query = query.where(or_(*(
                safunc.lower(CoreDataItem.english_value).contains(k.lower())
                for k in keywords
            )))
        items = session.execute(query).all()

        total_target = len(items) * len([l for l in target_langs if l != "en"])
        processed = 0
        logger.info(
            f"[retranslate_core] core={core_id} name={core_name!r} mode={mode} "
            f"items={len(items)} langs={target_langs} target={total_target}"
        )

        for item_id, english_value in items:
            for lang in target_langs:
                if lang == "en":
                    continue

                existing = session.execute(
                    select(CoreDataTranslation).where(
                        CoreDataTranslation.item_id == item_id,
                        CoreDataTranslation.language_code == lang,
                    )
                ).scalar_one_or_none()

                if existing and existing.validation_status == ValidationStatus.EXPERT_VALIDATED and not overwrite_expert:
                    continue

                processed_text = _process_text(english_value, "en", lang, mode, core_name, core_description)
                if not processed_text:
                    continue

                if existing:
                    existing.translated_value = processed_text
                    existing.validation_status = ValidationStatus.MACHINE_GENERATED
                else:
                    session.add(CoreDataTranslation(
                        item_id=item_id,
                        language_code=lang,
                        translated_value=processed_text,
                        validation_status=ValidationStatus.MACHINE_GENERATED,
                    ))
                processed += 1
                if processed % COMMIT_EVERY == 0:
                    session.commit()
                    logger.info(
                        f"[retranslate_core] core={core_id} progress "
                        f"{processed}/{total_target}"
                    )

        # Final partial batch
        session.commit()
    logger.info(f"[retranslate_core] DONE core={core_id} mode={mode} processed={processed}")


@celery_app.task(name="app.tasks.translation.translate_new_language_for_core", bind=True)
def translate_new_language_for_core(self, core_id: str, language_code: str):
    """
    When a new language is added to a Core — process all existing active items
    for that language only. Commits in batches like retranslate_core.
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus, StatusEnum

    engine = _get_sync_engine()

    with Session(engine) as session:
        mode, core_name, core_description = _get_core_context(session, core_id)
        items = session.execute(
            select(CoreDataItem.id, CoreDataItem.english_value).where(
                CoreDataItem.core_id == core_id,
                CoreDataItem.status == StatusEnum.ACTIVE,
            )
        ).all()

        total = len(items)
        processed = 0
        logger.info(
            f"[translate_new_language_for_core] core={core_id} name={core_name!r} mode={mode} "
            f"lang={language_code} items={total}"
        )

        for item_id, english_value in items:
            existing = session.execute(
                select(CoreDataTranslation).where(
                    CoreDataTranslation.item_id == item_id,
                    CoreDataTranslation.language_code == language_code,
                )
            ).scalar_one_or_none()

            if existing:
                continue

            processed_text = _process_text(english_value, "en", language_code, mode, core_name, core_description)
            if processed_text:
                session.add(CoreDataTranslation(
                    item_id=item_id,
                    language_code=language_code,
                    translated_value=processed_text,
                    validation_status=ValidationStatus.MACHINE_GENERATED,
                ))
                processed += 1
                if processed % COMMIT_EVERY == 0:
                    session.commit()
                    logger.info(
                        f"[translate_new_language_for_core] core={core_id} "
                        f"lang={language_code} progress {processed}/{total}"
                    )

        session.commit()
    logger.info(
        f"[translate_new_language_for_core] DONE core={core_id} "
        f"lang={language_code} mode={mode} processed={processed}"
    )
