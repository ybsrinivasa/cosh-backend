import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    String, Text, Boolean, Integer, DateTime, ForeignKey,
    UniqueConstraint, Enum as SAEnum, DECIMAL, JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base

import enum


# ── Enums ──────────────────────────────────────────────────────────────────────

class UserRole(str, enum.Enum):
    ADMIN = "ADMIN"
    DESIGNER = "DESIGNER"
    STOCKER = "STOCKER"
    REVIEWER = "REVIEWER"


class StatusEnum(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class CoreType(str, enum.Enum):
    TEXT = "TEXT"
    MEDIA = "MEDIA"


class ContentType(str, enum.Enum):
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    AUDIO = "AUDIO"
    DOCUMENT = "DOCUMENT"


class LanguageMode(str, enum.Enum):
    TRANSLATION = "TRANSLATION"
    TRANSLITERATION = "TRANSLITERATION"


class ValidationStatus(str, enum.Enum):
    MACHINE_GENERATED = "MACHINE_GENERATED"
    EXPERT_VALIDATED = "EXPERT_VALIDATED"


class SyncMode(str, enum.Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class SyncStatus(str, enum.Enum):
    DISPATCHED = "DISPATCHED"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class ChangeType(str, enum.Enum):
    ADDED = "ADDED"
    UPDATED = "UPDATED"
    INACTIVATED = "INACTIVATED"
    REACTIVATED = "REACTIVATED"
    TRANSLATION_UPDATED = "TRANSLATION_UPDATED"


class EntityType(str, enum.Enum):
    CORE = "CORE"
    CONNECT = "CONNECT"
    CORE_DATA_ITEM = "CORE_DATA_ITEM"
    CONNECT_DATA_ITEM = "CONNECT_DATA_ITEM"
    TRANSLATION = "TRANSLATION"


class SimilarityReason(str, enum.Enum):
    EXACT_DUPLICATE = "EXACT_DUPLICATE"
    FORMAT_DIFFERENCE = "FORMAT_DIFFERENCE"
    SPELLING_ERROR = "SPELLING_ERROR"
    REARRANGED_WORDS = "REARRANGED_WORDS"
    MISSING_WORDS = "MISSING_WORDS"
    ABBREVIATION = "ABBREVIATION"


class SimilarityStatus(str, enum.Enum):
    PENDING = "PENDING"
    KEEP_BOTH = "KEEP_BOTH"
    REMOVE_ONE = "REMOVE_ONE"
    MERGED = "MERGED"
    IGNORED = "IGNORED"


class TextDirection(str, enum.Enum):
    LTR = "LTR"
    RTL = "RTL"


def utcnow():
    return datetime.now(timezone.utc)


def new_uuid():
    return str(uuid.uuid4())


# ── 3.1 Users ─────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    roles: Mapped[list["UserRoleModel"]] = relationship("UserRoleModel", back_populates="user")


class UserRoleModel(Base):
    __tablename__ = "user_roles"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    role: Mapped[UserRole] = mapped_column(SAEnum(UserRole), nullable=False)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    user: Mapped["User"] = relationship("User", back_populates="roles")

    __table_args__ = (UniqueConstraint("user_id", "role"),)


# ── 3.2 Knowledge Structure ───────────────────────────────────────────────────

class Folder(Base):
    __tablename__ = "folders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    created_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    cores: Mapped[list["Core"]] = relationship("Core", back_populates="folder")


class Core(Base):
    __tablename__ = "cores"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    folder_id: Mapped[str] = mapped_column(String(36), ForeignKey("folders.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    core_type: Mapped[CoreType] = mapped_column(SAEnum(CoreType), nullable=False)
    content_type: Mapped[ContentType] = mapped_column(SAEnum(ContentType), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    language_mode: Mapped[LanguageMode] = mapped_column(SAEnum(LanguageMode), nullable=True)
    legacy_core_id: Mapped[str] = mapped_column(String(50), nullable=True)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    created_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    assigned_stocker_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    folder: Mapped["Folder"] = relationship("Folder", back_populates="cores")
    data_items: Mapped[list["CoreDataItem"]] = relationship("CoreDataItem", back_populates="core")
    product_tags: Mapped[list["CoreProductTag"]] = relationship("CoreProductTag", back_populates="core")
    language_configs: Mapped[list["CoreLanguageConfig"]] = relationship("CoreLanguageConfig", back_populates="core")


class CoreProductTag(Base):
    __tablename__ = "core_product_tags"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    core_id: Mapped[str] = mapped_column(String(36), ForeignKey("cores.id"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("product_registry.id"), nullable=False)

    core: Mapped["Core"] = relationship("Core", back_populates="product_tags")

    __table_args__ = (UniqueConstraint("core_id", "product_id"),)


class CoreLanguageConfig(Base):
    __tablename__ = "core_language_configs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    core_id: Mapped[str] = mapped_column(String(36), ForeignKey("cores.id"), nullable=False)
    language_code: Mapped[str] = mapped_column(String(10), nullable=False)

    core: Mapped["Core"] = relationship("Core", back_populates="language_configs")

    __table_args__ = (UniqueConstraint("core_id", "language_code"),)


class CoreDataItem(Base):
    __tablename__ = "core_data_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    core_id: Mapped[str] = mapped_column(String(36), ForeignKey("cores.id"), nullable=False)
    english_value: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    legacy_item_id: Mapped[str] = mapped_column(String(50), nullable=True)
    created_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    core: Mapped["Core"] = relationship("Core", back_populates="data_items")
    translations: Mapped[list["CoreDataTranslation"]] = relationship("CoreDataTranslation", back_populates="item")
    media_item: Mapped["MediaItem"] = relationship("MediaItem", back_populates="item", uselist=False)


class CoreDataTranslation(Base):
    __tablename__ = "core_data_translations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    item_id: Mapped[str] = mapped_column(String(36), ForeignKey("core_data_items.id"), nullable=False)
    language_code: Mapped[str] = mapped_column(String(10), nullable=False)
    translated_value: Mapped[str] = mapped_column(Text, nullable=False)
    validation_status: Mapped[ValidationStatus] = mapped_column(
        SAEnum(ValidationStatus), default=ValidationStatus.MACHINE_GENERATED
    )
    validated_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    validated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    item: Mapped["CoreDataItem"] = relationship("CoreDataItem", back_populates="translations")

    __table_args__ = (UniqueConstraint("item_id", "language_code"),)


class MediaItem(Base):
    __tablename__ = "media_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    item_id: Mapped[str] = mapped_column(String(36), ForeignKey("core_data_items.id"), nullable=False, unique=True)
    s3_url: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[ContentType] = mapped_column(SAEnum(ContentType), nullable=True)
    language_code: Mapped[str] = mapped_column(String(10), nullable=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=True)

    item: Mapped["CoreDataItem"] = relationship("CoreDataItem", back_populates="media_item")


# ── 3.3 Connects ──────────────────────────────────────────────────────────────

class Connect(Base):
    __tablename__ = "connects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    schema_finalised: Mapped[bool] = mapped_column(Boolean, default=False)
    created_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    assigned_stocker_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    product_tags: Mapped[list["ConnectProductTag"]] = relationship("ConnectProductTag", back_populates="connect")
    schema_positions: Mapped[list["ConnectSchemaPosition"]] = relationship("ConnectSchemaPosition", back_populates="connect")
    data_items: Mapped[list["ConnectDataItem"]] = relationship("ConnectDataItem", back_populates="connect")


class ConnectProductTag(Base):
    __tablename__ = "connect_product_tags"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    connect_id: Mapped[str] = mapped_column(String(36), ForeignKey("connects.id"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("product_registry.id"), nullable=False)

    connect: Mapped["Connect"] = relationship("Connect", back_populates="product_tags")

    __table_args__ = (UniqueConstraint("connect_id", "product_id"),)


class ConnectSchemaPosition(Base):
    __tablename__ = "connect_schema_positions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    connect_id: Mapped[str] = mapped_column(String(36), ForeignKey("connects.id"), nullable=False)
    position_number: Mapped[int] = mapped_column(Integer, nullable=False)
    core_id: Mapped[str] = mapped_column(String(36), ForeignKey("cores.id"), nullable=False)
    relationship_type_to_next: Mapped[str] = mapped_column(String(200), nullable=True)

    connect: Mapped["Connect"] = relationship("Connect", back_populates="schema_positions")

    __table_args__ = (UniqueConstraint("connect_id", "position_number"),)


class ConnectDataItem(Base):
    __tablename__ = "connect_data_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    connect_id: Mapped[str] = mapped_column(String(36), ForeignKey("connects.id"), nullable=False)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    legacy_connect_data_id: Mapped[str] = mapped_column(String(50), nullable=True)
    created_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    connect: Mapped["Connect"] = relationship("Connect", back_populates="data_items")
    positions: Mapped[list["ConnectDataPosition"]] = relationship("ConnectDataPosition", back_populates="data_item")


class ConnectDataPosition(Base):
    __tablename__ = "connect_data_positions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    connect_data_item_id: Mapped[str] = mapped_column(String(36), ForeignKey("connect_data_items.id"), nullable=False)
    position_number: Mapped[int] = mapped_column(Integer, nullable=False)
    core_data_item_id: Mapped[str] = mapped_column(String(36), ForeignKey("core_data_items.id"), nullable=False)

    data_item: Mapped["ConnectDataItem"] = relationship("ConnectDataItem", back_populates="positions")

    __table_args__ = (UniqueConstraint("connect_data_item_id", "position_number"),)


# ── 3.4 Registries ────────────────────────────────────────────────────────────

class LanguageRegistry(Base):
    __tablename__ = "language_registry"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    language_code: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    language_name_en: Mapped[str] = mapped_column(String(100), nullable=False)
    language_name_native: Mapped[str] = mapped_column(String(100), nullable=False)
    script: Mapped[str] = mapped_column(String(100), nullable=False)
    direction: Mapped[TextDirection] = mapped_column(SAEnum(TextDirection), default=TextDirection.LTR)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    added_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class RelationshipTypeRegistry(Base):
    __tablename__ = "relationship_type_registry"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    label: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    example: Mapped[str] = mapped_column(Text, nullable=True)
    added_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProductRegistry(Base):
    __tablename__ = "product_registry"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    sync_endpoint_url: Mapped[str] = mapped_column(Text, nullable=True)
    sync_api_key_secret_name: Mapped[str] = mapped_column(String(200), nullable=True)
    status: Mapped[StatusEnum] = mapped_column(SAEnum(StatusEnum), default=StatusEnum.ACTIVE)
    added_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


# ── 3.5 Sync Management ───────────────────────────────────────────────────────

class ProductSyncState(Base):
    __tablename__ = "product_sync_state"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("product_registry.id"), unique=True, nullable=False)
    last_successful_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_mode: Mapped[SyncMode] = mapped_column(SAEnum(SyncMode), nullable=True)
    last_sync_id: Mapped[str] = mapped_column(String(36), nullable=True)


class SyncChangeLog(Base):
    __tablename__ = "sync_change_log"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("product_registry.id"), nullable=False)
    entity_type: Mapped[EntityType] = mapped_column(SAEnum(EntityType), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(36), nullable=False)
    change_type: Mapped[ChangeType] = mapped_column(SAEnum(ChangeType), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    included_in_sync_id: Mapped[str] = mapped_column(String(36), nullable=True)


class SyncHistory(Base):
    __tablename__ = "sync_history"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    product_id: Mapped[str] = mapped_column(String(36), ForeignKey("product_registry.id"), nullable=False)
    sync_mode: Mapped[SyncMode] = mapped_column(SAEnum(SyncMode), nullable=False)
    initiated_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    initiated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    status: Mapped[SyncStatus] = mapped_column(SAEnum(SyncStatus), nullable=False)
    total_items: Mapped[int] = mapped_column(Integer, nullable=True)
    items_inserted: Mapped[int] = mapped_column(Integer, nullable=True)
    items_updated: Mapped[int] = mapped_column(Integer, nullable=True)
    items_failed: Mapped[int] = mapped_column(Integer, nullable=True)
    product_response: Mapped[dict] = mapped_column(JSON, nullable=True)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)


# ── 3.6 Similarity Review ─────────────────────────────────────────────────────

class SimilarityPair(Base):
    __tablename__ = "similarity_pairs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    item_id_a: Mapped[str] = mapped_column(String(36), ForeignKey("core_data_items.id"), nullable=False)
    item_id_b: Mapped[str] = mapped_column(String(36), ForeignKey("core_data_items.id"), nullable=False)
    similarity_score: Mapped[float] = mapped_column(DECIMAL(5, 4), nullable=False)
    similarity_reason: Mapped[SimilarityReason] = mapped_column(SAEnum(SimilarityReason), nullable=True)
    status: Mapped[SimilarityStatus] = mapped_column(SAEnum(SimilarityStatus), default=SimilarityStatus.PENDING)
    reviewed_by: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    reviewed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    merged_canonical_value: Mapped[str] = mapped_column(Text, nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (UniqueConstraint("item_id_a", "item_id_b"),)


# ── 3.7 Audit Log ─────────────────────────────────────────────────────────────

class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), nullable=False)
    action: Mapped[str] = mapped_column(String(200), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=True)
    entity_id: Mapped[str] = mapped_column(String(36), nullable=True)
    entity_name: Mapped[str] = mapped_column(String(500), nullable=True)
    details: Mapped[dict] = mapped_column(JSON, nullable=True)
    performed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
