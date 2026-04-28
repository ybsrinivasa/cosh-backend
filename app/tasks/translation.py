"""
Celery translation tasks — all async, never block the API.
Uses synchronous SQLAlchemy since Celery workers are not async.
"""
import logging
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session
from app.celery_app import celery_app
from app.services.translation_service import translate_text

logger = logging.getLogger(__name__)


def _get_sync_engine():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    url = os.getenv("DATABASE_URL_SYNC")
    return create_engine(url)


@celery_app.task(name="app.tasks.translation.translate_item", bind=True, max_retries=3)
def translate_item(self, item_id: str, english_value: str, target_langs: list[str]):
    """
    Translate one Core Data Item into all configured languages.
    Triggered after item creation or English value edit.
    Only overwrites MACHINE_GENERATED rows — never EXPERT_VALIDATED.
    """
    from app.models.models import CoreDataTranslation, ValidationStatus

    engine = _get_sync_engine()

    with Session(engine) as session:
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

            translated = translate_text(english_value, "en", lang)
            if not translated:
                continue

            if existing:
                existing.translated_value = translated
                existing.validation_status = ValidationStatus.MACHINE_GENERATED
            else:
                session.add(CoreDataTranslation(
                    item_id=item_id,
                    language_code=lang,
                    translated_value=translated,
                    validation_status=ValidationStatus.MACHINE_GENERATED,
                ))

        # BL-C-07: record TRANSLATION_UPDATED in sync_change_log
        from app.models.models import CoreDataItem, CoreProductTag, SyncChangeLog, EntityType, ChangeType
        item_row = session.execute(select(CoreDataItem).where(CoreDataItem.id == item_id)).scalar_one_or_none()
        if item_row:
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
    logger.info(f"Translation complete for item {item_id}")


@celery_app.task(name="app.tasks.translation.retranslate_core", bind=True, max_retries=2)
def retranslate_core(self, core_id: str, target_langs: list[str], overwrite_expert: bool = False):
    """
    Re-translate all items in a Core.
    overwrite_expert=False: skip EXPERT_VALIDATED (safe default).
    overwrite_expert=True: overwrite everything (requires Designer confirmation).
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus, StatusEnum

    engine = _get_sync_engine()

    with Session(engine) as session:
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

                translated = translate_text(item.english_value, "en", lang)
                if not translated:
                    continue

                if existing:
                    existing.translated_value = translated
                    existing.validation_status = ValidationStatus.MACHINE_GENERATED
                else:
                    session.add(CoreDataTranslation(
                        item_id=item.id,
                        language_code=lang,
                        translated_value=translated,
                        validation_status=ValidationStatus.MACHINE_GENERATED,
                    ))

        session.commit()
    logger.info(f"Re-translation complete for core {core_id}")


@celery_app.task(name="app.tasks.translation.translate_new_language_for_core", bind=True)
def translate_new_language_for_core(self, core_id: str, language_code: str):
    """
    When a new language is added to a Core — translate all existing active items for that language only.
    """
    from app.models.models import CoreDataItem, CoreDataTranslation, ValidationStatus, StatusEnum

    engine = _get_sync_engine()

    with Session(engine) as session:
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

            translated = translate_text(item.english_value, "en", language_code)
            if translated:
                session.add(CoreDataTranslation(
                    item_id=item.id,
                    language_code=language_code,
                    translated_value=translated,
                    validation_status=ValidationStatus.MACHINE_GENERATED,
                ))

        session.commit()
    logger.info(f"New language [{language_code}] translation complete for core {core_id}")
