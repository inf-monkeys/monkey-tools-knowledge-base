import uuid
from core.middleware.db import db
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import (
    String,
)

built_in_fields = [
    {
        "displayName": "文档 ID",
        "name": "document_id",
    },
    {
        "displayName": "创建时间",
        "name": "created_at",
    },
    {
        "displayName": "创建用户 ID",
        "name": "user_id",
    },
    {
        "displayName": "文件名称",
        "name": "filename",
    },
]


class MetadataFieldEntity(db.Model):
    __tablename__ = f"monkey_tools_knowledge_base_metadata_fields"
    __table_args__ = (db.PrimaryKeyConstraint("id", name="metadata_fields_pkey"),)
    id = db.Column(UUID)
    created_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    updated_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    knowledge_base_id = db.Column(UUID)
    key = db.Column(String)

    @staticmethod
    def find_by_knowledge_base_id(knowledge_base_id: str):
        return (
            MetadataFieldEntity.query.filter_by(knowledge_base_id=knowledge_base_id)
            .order_by(MetadataFieldEntity.created_at.desc())
            .all()
        )

    @staticmethod
    def add_keys_if_not_exists(knowledge_base_id: str, keys: list):
        built_in_keys = [field["name"] for field in built_in_fields]
        existing_keys = MetadataFieldEntity.find_by_knowledge_base_id(knowledge_base_id)
        existing_keys = [key.key for key in existing_keys]
        keys_to_add = list(set(keys) - set(existing_keys) - set(built_in_keys))
        for key in keys_to_add:
            metadata_field = MetadataFieldEntity(
                id=str(uuid.uuid4()), knowledge_base_id=knowledge_base_id, key=key
            )
            db.session.add(metadata_field)
        db.session.commit()
        return keys_to_add
