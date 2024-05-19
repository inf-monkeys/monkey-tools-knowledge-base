from sqlalchemy import (
    Column,
    Boolean,
)
from sqlalchemy.dialects.postgresql import UUID
from core.middleware.db import db


class SqlKnowledgeBaseEntity(db.Model):
    __tablename__ = f"monkey_tools_knowledge_bases_sql_knowledge_bases"
    __table_args__ = (
        db.PrimaryKeyConstraint("id", name="knowledge_base_sql_knowledge_base_pkey"),
    )
    id = db.Column(UUID)
    created_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    updated_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    is_deleted = Column(Boolean, default=False)
    type = db.Column(db.String(255), nullable=True, default="builtIn")

    # For external database
    database_type = db.Column(db.String(255), nullable=True)
    host = db.Column(db.String(255), nullable=True)
    port = db.Column(db.Integer, nullable=True)
    username = db.Column(db.String(255), nullable=True)
    password = db.Column(db.String(255), nullable=True)
    schema = db.Column(db.String(255), nullable=True, default="public")
    database = db.Column(db.String(255), nullable=True)

    def serialize(self):
        return {
            "id": self.id,
        }

    @staticmethod
    def get_by_id(id: str):
        knowledge_base = SqlKnowledgeBaseEntity.query.filter_by(id=id).first()
        if not knowledge_base:
            raise ValueError(f"Sql Knowledge base with id {id} not found")
        return knowledge_base

    @staticmethod
    def gene_database_name_by_id(knowledge_base_id: str) -> str:
        return knowledge_base_id

    @staticmethod
    def delete_by_id(id: str):
        knowledge_base = SqlKnowledgeBaseEntity.query.filter_by(id=id).first()
        if not knowledge_base:
            return False
        db.session.delete(knowledge_base)
        db.session.commit()
        return True
