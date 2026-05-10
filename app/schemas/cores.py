from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from app.models.models import CoreType, ContentType, LanguageMode, StatusEnum


class CoreCreate(BaseModel):
    folder_id: str
    name: str
    core_type: CoreType
    content_type: Optional[ContentType] = None
    description: Optional[str] = None
    language_mode: Optional[LanguageMode] = None


class CoreUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    language_mode: Optional[LanguageMode] = None
    assigned_stocker_id: Optional[str] = None


class CoreStatusUpdate(BaseModel):
    status: StatusEnum


class CoreOut(BaseModel):
    id: str
    folder_id: str
    name: str
    core_type: CoreType
    content_type: Optional[ContentType]
    description: Optional[str]
    language_mode: Optional[LanguageMode]
    status: StatusEnum
    is_public: bool = False
    legacy_core_id: Optional[str]
    assigned_stocker_id: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class CoreLanguageConfigOut(BaseModel):
    id: str
    core_id: str
    language_code: str

    class Config:
        from_attributes = True


class CoreProductTagOut(BaseModel):
    id: str
    core_id: str
    product_id: str
    entity_type_label: Optional[str] = None

    class Config:
        from_attributes = True


class CoreDataItemCreate(BaseModel):
    english_value: str
    s3_url: Optional[str] = None


class CoreDataItemUpdate(BaseModel):
    english_value: str
    s3_url: Optional[str] = None


class CoreDataItemStatusUpdate(BaseModel):
    status: StatusEnum


class TranslationOut(BaseModel):
    id: str
    item_id: str
    language_code: str
    translated_value: str
    validation_status: str
    validated_at: Optional[datetime]

    class Config:
        from_attributes = True


class CoreDataItemOut(BaseModel):
    id: str
    core_id: str
    english_value: str
    status: StatusEnum
    legacy_item_id: Optional[str]
    created_by_name: Optional[str] = None
    s3_url: Optional[str] = None
    created_at: datetime
    translations: List[TranslationOut] = []

    class Config:
        from_attributes = True


class BulkUploadReport(BaseModel):
    total_rows: int
    created: int
    skipped_duplicates: int
    translations_imported: int = 0
    errors: List[str]


class CoreDuplicateRow(BaseModel):
    id: str
    english_value: str
    created_at: Optional[str] = None
    legacy_created_by_name: Optional[str] = None


class CoreDuplicateGroup(BaseModel):
    key: str            # lowercased english_value used for grouping
    display_value: str  # actual english_value of the first row (for display)
    count: int
    rows: List[CoreDuplicateRow]


class CoreDuplicatesResponse(BaseModel):
    total_groups: int
    total_extra_items: int
    skip: int
    limit: int
    groups: List[CoreDuplicateGroup]


class CoreDuplicateCleanupRequest(BaseModel):
    key: Optional[str] = None
    all: bool = False


class CoreDuplicateCleanupResponse(BaseModel):
    groups_processed: int
    items_inactivated: int
    has_more: bool = False
    remaining: int = 0
