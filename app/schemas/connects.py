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
    id: str
    connect_data_item_id: str
    position_number: int
    core_data_item_id: Optional[str] = None
    connect_data_item_ref_id: Optional[str] = None

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
