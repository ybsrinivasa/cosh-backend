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

logger = logging.getLogger(__name__)


def _get_sync_engine():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    url = os.getenv("DATABASE_URL_SYNC")
    return create_engine(url)


def _process_text(text: str, source_lang: str, target_lang: str, mode) -> str | None:
    """Route to the right engine based on the Core's language_mode.

    `mode` is a LanguageMode enum value or None. None defaults to
    TRANSLATION so existing Cores keep their current behaviour.
    """
    from app.models.models import LanguageMode
    if mode == LanguageMode.TRANSLITERATION:
        return transliterate_text(text, source_lang, target_lang)
    return translate_text(text, source_lang, target_lang)


def _get_core_mode(session: Session, core_id: str):
    """Look up the Core's language_mode. Returns None if not found
    (caller treats None as "use translation")."""
    from app.models.models import Core
    core = session.execute(select(Core).where(Core.id == core_id)).scalar_one_or_none()
    return core.language_mode if core else None


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
        mode = _get_core_mode(session, item_row.core_id)

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

            processed = _process_text(english_value, "en", lang, mode)
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
def retranslate_core(self, core_id: str, target_langs: list[str], overwrite_expert: bool = False):
    """
    Re-process all items in a Core.
    overwrite_expert=False: skip EXPERT_VALIDATED (safe default).
    overwrite_expert=True: overwrite everything (requires Designer confirmation).
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus, StatusEnum

    engine = _get_sync_engine()

    with Session(engine) as session:
        mode = _get_core_mode(session, core_id)
        items = session.execute(
            select(CoreDataItem).where(
                CoreDataItem.core_id == core_id,
                CoreDataItem.status == StatusEnum.ACTIVE,
            )
        ).scalars().all()

        for item in items:
            for lang in target_langs:
                if lang == "en":
                    continue

                existing = session.execute(
                    select(CoreDataTranslation).where(
                        CoreDataTranslation.item_id == item.id,
                        CoreDataTranslation.language_code == lang,
                    )
                ).scalar_one_or_none()

                if existing and existing.validation_status == ValidationStatus.EXPERT_VALIDATED and not overwrite_expert:
                    continue

                processed = _process_text(item.english_value, "en", lang, mode)
                if not processed:
                    continue

                if existing:
                    existing.translated_value = processed
                    existing.validation_status = ValidationStatus.MACHINE_GENERATED
                else:
                    session.add(CoreDataTranslation(
                        item_id=item.id,
                        language_code=lang,
                        translated_value=processed,
                        validation_status=ValidationStatus.MACHINE_GENERATED,
                    ))

        session.commit()
    logger.info(f"Re-process complete for core {core_id} (mode={mode})")


@celery_app.task(name="app.tasks.translation.translate_new_language_for_core", bind=True)
def translate_new_language_for_core(self, core_id: str, language_code: str):
    """
    When a new language is added to a Core — process all existing active items
    for that language only.
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus, StatusEnum

    engine = _get_sync_engine()

    with Session(engine) as session:
        mode = _get_core_mode(session, core_id)
        items = session.execute(
            select(CoreDataItem).where(
                CoreDataItem.core_id == core_id,
                CoreDataItem.status == StatusEnum.ACTIVE,
            )
        ).scalars().all()

        for item in items:
            existing = session.execute(
                select(CoreDataTranslation).where(
                    CoreDataTranslation.item_id == item.id,
                    CoreDataTranslation.language_code == language_code,
                )
            ).scalar_one_or_none()

            if existing:
                continue

            processed = _process_text(item.english_value, "en", language_code, mode)
            if processed:
                session.add(CoreDataTranslation(
                    item_id=item.id,
                    language_code=language_code,
                    translated_value=processed,
                    validation_status=ValidationStatus.MACHINE_GENERATED,
                ))

        session.commit()
    logger.info(f"New language [{language_code}] processed for core {core_id} (mode={mode})")
