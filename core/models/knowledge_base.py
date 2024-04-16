from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
)
from sqlalchemy.dialects.postgresql import UUID
from core.middleware.db import db


class KnowledgeBaseEntity(db.Model):
    __tablename__ = f"monkey_tools_knowledge_bases"
    __table_args__ = (db.PrimaryKeyConstraint("id", name="knowledge_base_pkey"),)
    id = db.Column(UUID)
    created_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    updated_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    is_deleted = Column(Boolean, default=False)
    embedding_model = Column(String)
    dimension = Column(Integer)

    @staticmethod
    def gen_collection_name_by_id(dataset_id: str) -> str:
        normalized_dataset_id = dataset_id.replace("-", "_")
        return f"vector_index_{normalized_dataset_id}".lower()

    def serialize(self):
        return {
            "id": self.id,
            "embeddingModel": self.embedding_model,
            "dimension": self.dimension,
        }

    @staticmethod
    def get_by_id(id: str):
        knowledge_base = KnowledgeBaseEntity.query.filter_by(id=id).first()
        if not knowledge_base:
            raise ValueError(f"Knowledge base with id {id} not found")
        return knowledge_base
