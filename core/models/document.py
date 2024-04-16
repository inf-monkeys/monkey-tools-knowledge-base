from typing import Optional
from pydantic import BaseModel, Field
from core.middleware.db import db
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import (
    String,
)

from core.models.task import TaskStatus


class Document(BaseModel):
    """Class for storing a piece of text and associated metadata."""

    page_content: str

    """Arbitrary metadata about the page content (e.g., source, relationships to other
        documents, etc.).
    """
    metadata: Optional[dict] = Field(default_factory=dict)


class DocumentEntity(db.Model):
    __tablename__ = f"monkey_tools_knowledge_base_documents"
    __table_args__ = (db.PrimaryKeyConstraint("id", name="document_pkey"),)
    id = db.Column(UUID)
    created_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    updated_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    knowledge_base_id = db.Column(UUID)
    index_status = db.Column(String)
    failed_message = db.Column(String)
    filename = db.Column(String)
    file_url = db.Column(String)

    def serialize(self):
        return {
            "id": self.id,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "knowledgeBaseId": self.knowledge_base_id,
            "failedMessage": self.failed_message,
            "indexStatus": self.index_status,
            "filename": self.filename,
            "fileUrl": self.file_url,
        }

    @staticmethod
    def find_by_knowledge_base_id(knowledge_base_id: str):
        return DocumentEntity.query.filter_by(knowledge_base_id=knowledge_base_id).order_by(DocumentEntity.created_at.desc()).all()

    @staticmethod
    def get_by_id(id: str):
        return DocumentEntity.query.filter_by(id=id).first()

    @staticmethod
    def update_status_by_id(
        task_id: str, index_status: TaskStatus, failed_message: str = None
    ):
        document = DocumentEntity.query.filter_by(id=task_id).first()
        document.index_status = index_status.value
        document.failed_message = failed_message
        db.session.commit()
        return document

    @staticmethod
    def delete_by_id(id: str):
        document = DocumentEntity.query.filter_by(id=id).first()
        db.session.delete(document)
        db.session.commit()
        return document
