from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from app.models.models import StatusEnum


class ConnectCreate(BaseModel):
    name: str
    description: Optional[str] = None


class ConnectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    assigned_stocker_id: Optional[str] = None


class SchemaPositionIn(BaseModel):
    position_number: int
    node_type: str = 'CORE'
    core_id: Optional[str] = None
    connect_ref_id: Optional[str] = None
    relationship_type_to_next: Optional[str] = None
    position_label: Optional[str] = None


class SchemaPositionOut(BaseModel):
    id: str
    connect_id: str
    position_number: int
    node_type: str = 'CORE'
    core_id: Optional[str] = None
    core_name: Optional[str] = None
    connect_ref_id: Optional[str] = None
    connect_ref_name: Optional[str] = None
    relationship_type_to_next: Optional[str]
    position_label: Optional[str] = None

    class Config:
        from_attributes = True


class ConnectOut(BaseModel):
    id: str
    name: str
    description: Optional[str]
    status: StatusEnum
    schema_finalised: bool
    is_public: bool = False
    assigned_stocker_id: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class ConnectDataPositionIn(BaseModel):
    position_number: int
    core_data_item_id: Optional[str] = None
    connect_data_item_ref_id: Optional[str] = None


class ConnectDataPositionOut(BaseModel):
    position_number: int
    core_data_item_id: Optional[str] = None
    connect_data_item_ref_id: Optional[str] = None
    display_value: Optional[str] = None
    item_status: Optional[str] = None

    class Config:
        from_attributes = True


class ConnectDataItemOut(BaseModel):
    id: str
    connect_id: str
    status: StatusEnum
    created_by_name: Optional[str] = None
    created_at: datetime
    positions: List[ConnectDataPositionOut] = []

    class Config:
        from_attributes = True


class ConnectProductTagOut(BaseModel):
    id: str
    connect_id: str
    product_id: str
    entity_type_label: Optional[str] = None

    class Config:
        from_attributes = True


class ConnectStatusUpdate(BaseModel):
    status: StatusEnum


class ConnectDataStatusUpdate(BaseModel):
    status: StatusEnum


class ExcelUploadReport(BaseModel):
    total_rows: int
    resolved: int
    unresolved: int
    skipped_duplicates: int = 0
    unresolved_details: List[dict]


class DuplicatePositionValue(BaseModel):
    position_number: int
    label: str
    value: str


class DuplicateRow(BaseModel):
    cdi_id: str
    created_at: Optional[str] = None
    legacy_created_by_name: Optional[str] = None
    position_values: List[DuplicatePositionValue]


class DuplicateGroup(BaseModel):
    fingerprint: str
    count: int
    rows: List[DuplicateRow]


class DuplicatesResponse(BaseModel):
    total_groups: int
    total_extra_items: int
    skip: int
    limit: int
    groups: List[DuplicateGroup]


class DuplicateCleanupRequest(BaseModel):
    fingerprint: Optional[str] = None
    all: bool = False


class DuplicateCleanupResponse(BaseModel):
    groups_processed: int
    items_inactivated: int
    has_more: bool = False
    remaining: int = 0
