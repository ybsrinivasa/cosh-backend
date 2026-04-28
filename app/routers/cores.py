import csv
import io
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.database import get_db
from app.dependencies import require_role
from app.models.models import (
    Folder, Core, CoreDataItem, CoreDataTranslation, CoreLanguageConfig,
    CoreProductTag, ProductRegistry, LanguageRegistry,
    UserRole, StatusEnum, CoreType, ValidationStatus
)
from app.schemas.cores import (
    CoreCreate, CoreUpdate, CoreStatusUpdate, CoreOut,
    CoreDataItemCreate, CoreDataItemUpdate, CoreDataItemStatusUpdate,
    CoreDataItemOut, CoreLanguageConfigOut, CoreProductTagOut,
    BulkUploadReport, TranslationOut
)
from app.services.core_service import (
    name_is_unique_for_core, get_core, get_item,
    dual_write_create, dual_write_update_english, inactivity_cascade
)
from app.services.sync_service import write_sync_changes
from app.models.models import EntityType, ChangeType

router = APIRouter(prefix="/cores", tags=["Cores"])

require_designer = require_role(UserRole.DESIGNER, UserRole.ADMIN)
require_designer_or_stocker = require_role(UserRole.DESIGNER, UserRole.STOCKER, UserRole.ADMIN)


# ── Core CRUD ──────────────────────────────────────────────────────────────────

@router.get("", response_model=list[CoreOut])
async def list_cores(db: AsyncSession = Depends(get_db), _=Depends(require_designer_or_stocker)):
    result = await db.execute(select(Core).order_by(Core.name))
    return result.scalars().all()


@router.post("", response_model=CoreOut, status_code=status.HTTP_201_CREATED)
async def create_core(
    request: CoreCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer),
):
    if request.core_type == CoreType.MEDIA and not request.content_type:
        raise HTTPException(status_code=422, detail="content_type is required for MEDIA cores")

    if not await name_is_unique_for_core(db, request.name):
        raise HTTPException(status_code=409, detail=f"'{request.name}' is already used by a Folder or Core")

    folder = (await db.execute(select(Folder).where(Folder.id == request.folder_id))).scalar_one_or_none()
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")

    core = Core(
        folder_id=request.folder_id,
        name=request.name,
        core_type=request.core_type,
        content_type=request.content_type,
        description=request.description,
        language_mode=request.language_mode,
        created_by=current_user.id,
        status=StatusEnum.ACTIVE,
    )
    db.add(core)
    await db.commit()
    await db.refresh(core)
    return core


@router.get("/{core_id}", response_model=CoreOut)
async def get_core_detail(core_id: str, db: AsyncSession = Depends(get_db), _=Depends(require_designer_or_stocker)):
    return await get_core(db, core_id)


@router.put("/{core_id}", response_model=CoreOut)
async def update_core(
    core_id: str,
    request: CoreUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    core = await get_core(db, core_id)

    if request.name and request.name != core.name:
        if not await name_is_unique_for_core(db, request.name, exclude_core_id=core_id):
            raise HTTPException(status_code=409, detail=f"'{request.name}' is already used by a Folder or Core")
        core.name = request.name

    if request.description is not None:
        core.description = request.description
    if request.language_mode is not None:
        core.language_mode = request.language_mode
    if request.assigned_stocker_id is not None:
        core.assigned_stocker_id = request.assigned_stocker_id

    await db.commit()
    await db.refresh(core)
    return core


@router.put("/{core_id}/status", response_model=CoreOut)
async def update_core_status(
    core_id: str,
    request: CoreStatusUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    core = await get_core(db, core_id)
    core.status = request.status

    if request.status == StatusEnum.INACTIVE:
        items = (await db.execute(
            select(CoreDataItem).where(CoreDataItem.core_id == core_id, CoreDataItem.status == StatusEnum.ACTIVE)
        )).scalars().all()
        for item in items:
            item.status = StatusEnum.INACTIVE
            await inactivity_cascade(db, item.id)

    await db.commit()
    await db.refresh(core)
    return core


# ── Core Language Config ───────────────────────────────────────────────────────

@router.get("/{core_id}/languages", response_model=list[CoreLanguageConfigOut])
async def list_core_languages(core_id: str, db: AsyncSession = Depends(get_db), _=Depends(require_designer_or_stocker)):
    await get_core(db, core_id)
    result = await db.execute(select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id))
    return result.scalars().all()


@router.post("/{core_id}/languages", response_model=CoreLanguageConfigOut, status_code=status.HTTP_201_CREATED)
async def add_language_to_core(
    core_id: str,
    language_code: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    await get_core(db, core_id)

    lang = (await db.execute(
        select(LanguageRegistry).where(LanguageRegistry.language_code == language_code, LanguageRegistry.status == StatusEnum.ACTIVE)
    )).scalar_one_or_none()
    if not lang:
        raise HTTPException(status_code=404, detail=f"Language '{language_code}' not found in registry")

    existing = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id, CoreLanguageConfig.language_code == language_code)
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Language already configured for this Core")

    config = CoreLanguageConfig(core_id=core_id, language_code=language_code)
    db.add(config)
    await db.commit()
    await db.refresh(config)

    from app.tasks.translation import translate_new_language_for_core
    translate_new_language_for_core.delay(core_id, language_code)

    return config


@router.delete("/{core_id}/languages/{language_code}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_language_from_core(
    core_id: str,
    language_code: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    config = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id, CoreLanguageConfig.language_code == language_code)
    )).scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail="Language not configured for this Core")
    await db.delete(config)
    await db.commit()


# ── Core Product Tags ──────────────────────────────────────────────────────────

@router.get("/{core_id}/product-tags", response_model=list[CoreProductTagOut])
async def list_core_product_tags(core_id: str, db: AsyncSession = Depends(get_db), _=Depends(require_designer_or_stocker)):
    await get_core(db, core_id)
    result = await db.execute(select(CoreProductTag).where(CoreProductTag.core_id == core_id))
    return result.scalars().all()


@router.post("/{core_id}/product-tags", response_model=CoreProductTagOut, status_code=status.HTTP_201_CREATED)
async def tag_core_to_product(
    core_id: str,
    product_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    await get_core(db, core_id)

    product = (await db.execute(select(ProductRegistry).where(ProductRegistry.id == product_id))).scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    existing = (await db.execute(
        select(CoreProductTag).where(CoreProductTag.core_id == core_id, CoreProductTag.product_id == product_id)
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Core already tagged to this product")

    tag = CoreProductTag(core_id=core_id, product_id=product_id)
    db.add(tag)
    await db.commit()
    await db.refresh(tag)
    return tag


@router.delete("/{core_id}/product-tags/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_core_product_tag(
    core_id: str,
    product_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    tag = (await db.execute(
        select(CoreProductTag).where(CoreProductTag.core_id == core_id, CoreProductTag.product_id == product_id)
    )).scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")
    await db.delete(tag)
    await db.commit()


# ── Core Data Items ────────────────────────────────────────────────────────────

@router.get("/{core_id}/items", response_model=list[CoreDataItemOut])
async def list_items(
    core_id: str,
    status_filter: str = "ACTIVE",
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer_or_stocker),
):
    await get_core(db, core_id)
    q = select(CoreDataItem).options(selectinload(CoreDataItem.translations)).where(CoreDataItem.core_id == core_id)
    if status_filter in ("ACTIVE", "INACTIVE"):
        q = q.where(CoreDataItem.status == StatusEnum(status_filter))
    result = await db.execute(q.order_by(CoreDataItem.english_value))
    return result.scalars().all()


@router.post("/{core_id}/items", response_model=CoreDataItemOut, status_code=status.HTTP_201_CREATED)
async def create_item(
    core_id: str,
    request: CoreDataItemCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    core = await get_core(db, core_id)

    existing = (await db.execute(
        select(CoreDataItem).where(
            CoreDataItem.core_id == core_id,
            CoreDataItem.english_value.ilike(request.english_value)
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="This value already exists in the Core")

    item = CoreDataItem(
        core_id=core_id,
        english_value=request.english_value,
        status=StatusEnum.ACTIVE,
        created_by=current_user.id,
    )
    db.add(item)
    await db.flush()

    try:
        await dual_write_create(db, item)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Neo4J write failed: {str(e)}")

    await db.commit()
    await db.refresh(item)

    lang_configs = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id)
    )).scalars().all()
    if lang_configs:
        from app.tasks.translation import translate_item
        target_langs = [c.language_code for c in lang_configs]
        translate_item.delay(item.id, item.english_value, target_langs)

    # BL-C-07: record change for sync
    await write_sync_changes(db, EntityType.CORE_DATA_ITEM, item.id, ChangeType.ADDED, core_id=core_id)
    await db.commit()

    # BL-C-05: trigger targeted similarity check for TEXT cores
    if core.core_type == CoreType.TEXT:
        from app.tasks.similarity import check_item_similarity
        check_item_similarity.delay(item.id)

    result = await db.execute(
        select(CoreDataItem).options(selectinload(CoreDataItem.translations)).where(CoreDataItem.id == item.id)
    )
    return result.scalar_one()


@router.put("/{core_id}/items/{item_id}", response_model=CoreDataItemOut)
async def update_item(
    core_id: str,
    item_id: str,
    request: CoreDataItemUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer_or_stocker),
):
    item = await get_item(db, item_id)
    if item.core_id != core_id:
        raise HTTPException(status_code=404, detail="Item not found in this Core")

    item.english_value = request.english_value
    await dual_write_update_english(item_id, request.english_value)
    await write_sync_changes(db, EntityType.CORE_DATA_ITEM, item_id, ChangeType.UPDATED, core_id=core_id)
    await db.commit()
    await db.refresh(item)

    lang_configs = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id)
    )).scalars().all()
    if lang_configs:
        from app.tasks.translation import translate_item
        target_langs = [c.language_code for c in lang_configs]
        translate_item.delay(item.id, item.english_value, target_langs)

    return item


@router.put("/{core_id}/items/{item_id}/status", response_model=CoreDataItemOut)
async def update_item_status(
    core_id: str,
    item_id: str,
    request: CoreDataItemStatusUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer_or_stocker),
):
    item = await get_item(db, item_id)
    if item.core_id != core_id:
        raise HTTPException(status_code=404, detail="Item not found in this Core")

    item.status = request.status
    cascaded = 0
    if request.status == StatusEnum.INACTIVE:
        cascaded = await inactivity_cascade(db, item_id)
        change = ChangeType.INACTIVATED
    else:
        change = ChangeType.REACTIVATED

    await write_sync_changes(db, EntityType.CORE_DATA_ITEM, item_id, change, core_id=core_id)
    await db.commit()
    await db.refresh(item)
    return item


# ── Bulk CSV Upload ────────────────────────────────────────────────────────────

@router.post("/{core_id}/items/upload-csv", response_model=BulkUploadReport)
async def upload_csv(
    core_id: str,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer),
):
    """
    P2-06 / P7-02: Bulk CSV upload for TEXT cores.
    Columns: english_value (required), legacy_id (optional),
    plus any number of language columns: {lang}_value and {lang}_validation_status.
    Language columns allow migration of pre-translated content in one shot.
    Expert-validated translations are never overwritten by auto-translation.
    """
    core = await get_core(db, core_id)
    if core.core_type.value != "TEXT":
        raise HTTPException(status_code=422, detail="CSV upload is only for TEXT cores")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=422, detail="CSV file is empty or has no data rows")

    # Detect language columns: any header matching "{lang}_value"
    headers = reader.fieldnames or []
    lang_cols = {}  # lang_code → (value_col, status_col)
    for h in headers:
        if h.endswith("_value") and h != "english_value":
            lang = h[:-6]  # strip "_value"
            if len(lang) <= 10:  # plausible language code
                status_col = f"{lang}_validation_status"
                lang_cols[lang] = (h, status_col if status_col in headers else None)

    created = 0
    skipped = 0
    translations_imported = 0
    errors = []

    for i, row in enumerate(rows, start=2):
        english_value = (row.get("english_value") or row.get("English_value") or "").strip()
        if not english_value:
            errors.append(f"Row {i}: empty english_value — skipped")
            continue

        existing = (await db.execute(
            select(CoreDataItem).where(
                CoreDataItem.core_id == core_id,
                CoreDataItem.english_value.ilike(english_value)
            )
        )).scalar_one_or_none()

        if existing:
            skipped += 1
            # Still import translations for existing items if language columns present
            item = existing
        else:
            item = CoreDataItem(
                core_id=core_id,
                english_value=english_value,
                legacy_item_id=row.get("id") or row.get("legacy_id") or None,
                status=StatusEnum.ACTIVE,
                created_by=current_user.id,
            )
            db.add(item)
            await db.flush()

            try:
                await dual_write_create(db, item)
                created += 1
            except Exception as e:
                await db.rollback()
                errors.append(f"Row {i}: Neo4J write failed — {str(e)}")
                continue

        # Import language translations from CSV columns (P7-02)
        for lang, (val_col, status_col) in lang_cols.items():
            translated_value = (row.get(val_col) or "").strip()
            if not translated_value:
                continue

            raw_status = (row.get(status_col) or "").strip().upper() if status_col else ""
            is_expert = raw_status in ("EXPERT_VALIDATED", "TRUE", "1", "YES", "VALIDATED")
            validation_status = ValidationStatus.EXPERT_VALIDATED if is_expert else ValidationStatus.MACHINE_GENERATED

            existing_trans = (await db.execute(
                select(CoreDataTranslation).where(
                    CoreDataTranslation.item_id == item.id,
                    CoreDataTranslation.language_code == lang,
                )
            )).scalar_one_or_none()

            if existing_trans:
                # Never overwrite EXPERT_VALIDATED with machine-generated
                if existing_trans.validation_status == ValidationStatus.EXPERT_VALIDATED and not is_expert:
                    continue
                existing_trans.translated_value = translated_value
                existing_trans.validation_status = validation_status
            else:
                db.add(CoreDataTranslation(
                    item_id=item.id,
                    language_code=lang,
                    translated_value=translated_value,
                    validation_status=validation_status,
                ))
            translations_imported += 1

    await db.commit()

    # Only trigger auto-translation for languages NOT already populated by the CSV
    lang_configs = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id)
    )).scalars().all()
    if lang_configs and created > 0:
        langs_needing_translation = [c.language_code for c in lang_configs if c.language_code not in lang_cols]
        if langs_needing_translation:
            from app.tasks.translation import retranslate_core as retranslate_task
            retranslate_task.delay(core_id, langs_needing_translation, overwrite_expert=False)

    return BulkUploadReport(
        total_rows=len(rows),
        created=created,
        skipped_duplicates=skipped,
        translations_imported=translations_imported,
        errors=errors,
    )


# ── Translations ───────────────────────────────────────────────────────────────

@router.get("/{core_id}/items/{item_id}/translations", response_model=list[TranslationOut])
async def list_translations(
    core_id: str,
    item_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer_or_stocker),
):
    item = await get_item(db, item_id)
    if item.core_id != core_id:
        raise HTTPException(status_code=404, detail="Item not found in this Core")
    return item.translations


# ── Re-translation ─────────────────────────────────────────────────────────────

@router.put("/{core_id}/retranslate")
async def retranslate_core(
    core_id: str,
    mode: str = Query("machine_generated_only", description="machine_generated_only or all"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    """
    Trigger re-translation for all items in a Core.
    mode=machine_generated_only: safe — preserves EXPERT_VALIDATED translations.
    mode=all: overwrites everything including EXPERT_VALIDATED. Use with caution.
    """
    await get_core(db, core_id)

    if mode not in ("machine_generated_only", "all"):
        raise HTTPException(status_code=422, detail="mode must be 'machine_generated_only' or 'all'")

    lang_configs = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id)
    )).scalars().all()

    if not lang_configs:
        raise HTTPException(status_code=422, detail="No languages configured for this Core")

    target_langs = [c.language_code for c in lang_configs]
    overwrite_expert = (mode == "all")

    from app.tasks.translation import retranslate_core as retranslate_task
    retranslate_task.delay(core_id, target_langs, overwrite_expert)

    return {
        "message": f"Re-translation queued for {len(target_langs)} languages",
        "mode": mode,
        "languages": target_langs,
    }


# ── CSV Export (BL-C-08 step 1) ────────────────────────────────────────────────

@router.get("/{core_id}/export-translations")
async def export_translations_csv(
    core_id: str,
    lang: str = Query(..., description="BCP-47 language code, e.g. 'kn', 'hi'"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer_or_stocker),
):
    """
    Export language-specific CSV for expert correction.
    UTF-8-BOM encoded for Excel compatibility with Indian scripts.
    """
    core = await get_core(db, core_id)

    lang_config = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id, CoreLanguageConfig.language_code == lang)
    )).scalar_one_or_none()
    if not lang_config:
        raise HTTPException(status_code=404, detail=f"Language '{lang}' not configured for this Core")

    items = (await db.execute(
        select(CoreDataItem)
        .options(selectinload(CoreDataItem.translations))
        .where(CoreDataItem.core_id == core_id, CoreDataItem.status == StatusEnum.ACTIVE)
        .order_by(CoreDataItem.english_value)
    )).scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["core_data_item_id", "legacy_item_id", "english_value", lang, "validation_status"])

    for item in items:
        trans = next((t for t in item.translations if t.language_code == lang), None)
        writer.writerow([
            item.id,
            item.legacy_item_id or "",
            item.english_value,
            trans.translated_value if trans else "",
            trans.validation_status.value if trans else "MACHINE_GENERATED",
        ])

    csv_content = "﻿" + output.getvalue()

    return StreamingResponse(
        io.BytesIO(csv_content.encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={core.name}_{lang}.csv"},
    )


# ── CSV Import (BL-C-08 steps 3-7) ────────────────────────────────────────────

@router.post("/{core_id}/import-translations")
async def import_translations_csv(
    core_id: str,
    lang: str = Query(..., description="BCP-47 language code"),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer_or_stocker),
):
    """
    Import expert-corrected CSV. Matches rows by core_data_item_id (UUID).
    All uploaded rows are marked EXPERT_VALIDATED regardless of changes.
    """
    from app.models.models import ValidationStatus

    await get_core(db, core_id)

    lang_config = (await db.execute(
        select(CoreLanguageConfig).where(CoreLanguageConfig.core_id == core_id, CoreLanguageConfig.language_code == lang)
    )).scalar_one_or_none()
    if not lang_config:
        raise HTTPException(status_code=404, detail=f"Language '{lang}' not configured for this Core")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    updated = 0
    skipped = 0
    errors = []

    for row in rows:
        item_id = (row.get("core_data_item_id") or "").strip()
        translated_value = (row.get(lang) or "").strip()

        if not item_id:
            errors.append("Row missing core_data_item_id — skipped")
            continue

        item = (await db.execute(
            select(CoreDataItem).where(CoreDataItem.id == item_id, CoreDataItem.core_id == core_id)
        )).scalar_one_or_none()
        if not item:
            skipped += 1
            errors.append(f"ID {item_id}: not found in this Core — skipped")
            continue

        if not translated_value:
            skipped += 1
            continue

        existing = (await db.execute(
            select(CoreDataTranslation).where(
                CoreDataTranslation.item_id == item_id,
                CoreDataTranslation.language_code == lang,
            )
        )).scalar_one_or_none()

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)

        if existing:
            existing.translated_value = translated_value
            existing.validation_status = ValidationStatus.EXPERT_VALIDATED
            existing.validated_by = current_user.id
            existing.validated_at = now
        else:
            db.add(CoreDataTranslation(
                item_id=item_id,
                language_code=lang,
                translated_value=translated_value,
                validation_status=ValidationStatus.EXPERT_VALIDATED,
                validated_by=current_user.id,
                validated_at=now,
            ))
        updated += 1

    await db.commit()

    return {
        "total_rows": len(rows),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
    }
