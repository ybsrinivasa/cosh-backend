import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.dependencies import require_role
from app.models.models import (
    UserRole, ProductRegistry, CoreProductTag, ConnectProductTag,
    SyncMode, StatusEnum,
)
from app.schemas.sync import (
    ChangeTableResponse, DispatchRequest, DispatchResponse,
    SyncHistoryOut, ProductSyncStateOut,
)
from app.services.sync_service import (
    get_change_table, resolve_dispatch_entities,
    build_payload, create_sync_history, get_sync_history,
)

router = APIRouter(prefix="/sync", tags=["Sync Management"])
require_admin = require_role(UserRole.ADMIN)


@router.get("/products", response_model=list[ProductSyncStateOut])
async def list_sync_products(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """List all active products with their last sync state and pending change count."""
    from sqlalchemy import func
    from app.models.models import ProductSyncState, SyncChangeLog

    products = (await db.execute(
        select(ProductRegistry).where(ProductRegistry.status == StatusEnum.ACTIVE)
    )).scalars().all()

    result = []
    for product in products:
        state = (await db.execute(
            select(ProductSyncState).where(ProductSyncState.product_id == product.id)
        )).scalar_one_or_none()

        pending = (await db.execute(
            select(func.count()).select_from(SyncChangeLog).where(
                SyncChangeLog.product_id == product.id,
                SyncChangeLog.included_in_sync_id.is_(None),
            )
        )).scalar_one()

        result.append(ProductSyncStateOut(
            product_id=product.id,
            product_name=product.display_name,
            last_successful_sync_at=state.last_successful_sync_at if state else None,
            last_sync_mode=state.last_sync_mode if state else None,
            pending_changes=pending,
        ))

    return result


@router.get("/{product_id}/changes", response_model=ChangeTableResponse)
async def get_pending_changes(
    product_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    P6-02: Return the change table for a product — all pending changes grouped
    by Core or Connect. Computed on demand from sync_change_log.
    """
    return await get_change_table(db, product_id)


@router.post("/{product_id}/dispatch", response_model=DispatchResponse, status_code=status.HTTP_202_ACCEPTED)
async def dispatch_sync(
    product_id: str,
    request: DispatchRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_admin),
):
    """
    P6-02: Admin dispatches a sync to the product's endpoint.
    Validates entity selection, auto-adds dependencies, builds payload,
    and dispatches as a background Celery task.
    Returns sync_id immediately — check history for outcome.
    """
    product = (await db.execute(
        select(ProductRegistry).where(ProductRegistry.id == product_id)
    )).scalar_one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if not product.sync_endpoint_url:
        raise HTTPException(status_code=422, detail="Product has no sync endpoint configured")

    if not request.send_all and not request.entity_ids:
        raise HTTPException(status_code=422, detail="Provide entity_ids or set send_all=true")

    core_ids, connect_ids, auto_added = await resolve_dispatch_entities(
        db, product_id, request.entity_ids or [], request.send_all
    )

    if not core_ids and not connect_ids:
        raise HTTPException(status_code=422, detail="No tagged entities found for this product with the given selection")

    sync_id = str(uuid.uuid4())

    payload = await build_payload(
        db, product_id, core_ids, connect_ids,
        request.sync_mode, sync_id, current_user.id,
    )

    total_items = sum(len(b["items"]) for b in payload["entity_batches"])
    history = await create_sync_history(db, product_id, request.sync_mode, current_user.id, sync_id)

    # Retrieve API key — stored directly in secret_name field at local dev stage
    api_key = product.sync_api_key_secret_name or ""

    from app.tasks.sync import dispatch_to_product
    dispatch_to_product.delay(sync_id, payload, product.sync_endpoint_url, api_key)

    entity_count = len(core_ids) + len(connect_ids)
    return DispatchResponse(
        sync_id=sync_id,
        message=f"Sync dispatched: {total_items} items across {entity_count} entities",
        entity_count=entity_count,
        auto_added_dependencies=auto_added,
    )


@router.get("/{product_id}/history", response_model=list[SyncHistoryOut])
async def sync_history(
    product_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """P6-03: Return the last 20 sync operations for a product."""
    return await get_sync_history(db, product_id)


@router.get("/{product_id}/history/{sync_id}")
async def sync_history_detail(
    product_id: str,
    sync_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """P6-03: Return full detail (including product_response) for one sync operation."""
    from app.models.models import SyncHistory
    history = (await db.execute(
        select(SyncHistory).where(
            SyncHistory.id == sync_id,
            SyncHistory.product_id == product_id,
        )
    )).scalar_one_or_none()
    if not history:
        raise HTTPException(status_code=404, detail="Sync history record not found")
    return {
        "id": history.id,
        "product_id": history.product_id,
        "sync_mode": history.sync_mode,
        "status": history.status,
        "initiated_by": history.initiated_by,
        "initiated_at": history.initiated_at,
        "completed_at": history.completed_at,
        "total_items": history.total_items,
        "items_inserted": history.items_inserted,
        "items_updated": history.items_updated,
        "items_failed": history.items_failed,
        "product_response": history.product_response,
    }


@router.put("/{product_id}/entities/{entity_id}/label", status_code=status.HTTP_200_OK)
async def set_entity_type_label(
    product_id: str,
    entity_id: str,
    entity_type_label: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """
    Set the entity_type_label on a Core or Connect product tag.
    This label is used as the entity_type in the sync payload sent to RootsTalk.
    """
    core_tag = (await db.execute(
        select(CoreProductTag).where(
            CoreProductTag.core_id == entity_id,
            CoreProductTag.product_id == product_id,
        )
    )).scalar_one_or_none()

    if core_tag:
        core_tag.entity_type_label = entity_type_label
        await db.commit()
        return {"message": f"Core tag label set to '{entity_type_label}'"}

    connect_tag = (await db.execute(
        select(ConnectProductTag).where(
            ConnectProductTag.connect_id == entity_id,
            ConnectProductTag.product_id == product_id,
        )
    )).scalar_one_or_none()

    if connect_tag:
        connect_tag.entity_type_label = entity_type_label
        await db.commit()
        return {"message": f"Connect tag label set to '{entity_type_label}'"}

    raise HTTPException(status_code=404, detail="No product tag found for this entity and product")
