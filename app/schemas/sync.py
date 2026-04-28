from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime
from app.models.models import SyncMode, SyncStatus, ChangeType, EntityType


class ChangeTableEntry(BaseModel):
    entity_id: str
    entity_name: str
    entity_category: str
    change_types: List[str]
    item_count: int


class ChangeTableResponse(BaseModel):
    product_id: str
    product_name: str
    total_changed_entities: int
    entities: List[ChangeTableEntry]


class DispatchRequest(BaseModel):
    sync_mode: SyncMode
    entity_ids: Optional[List[str]] = None
    send_all: bool = False


class DispatchResponse(BaseModel):
    sync_id: str
    message: str
    entity_count: int
    auto_added_dependencies: List[str] = []


class SyncHistoryOut(BaseModel):
    id: str
    product_id: str
    sync_mode: SyncMode
    initiated_by: Optional[str]
    initiated_at: datetime
    status: SyncStatus
    total_items: Optional[int]
    items_inserted: Optional[int]
    items_updated: Optional[int]
    items_failed: Optional[int]
    completed_at: Optional[datetime]

    class Config:
        from_attributes = True


class ProductSyncStateOut(BaseModel):
    product_id: str
    product_name: str
    last_successful_sync_at: Optional[datetime]
    last_sync_mode: Optional[SyncMode]
    pending_changes: int
