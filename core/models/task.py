from sqlalchemy import (
    DECIMAL,
    String,
)
from sqlalchemy.dialects.postgresql import UUID
from core.middleware.db import db
from enum import Enum


class TaskStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskEntity(db.Model):
    __tablename__ = f"monkey_tools_knowledge_base_tasks"
    __table_args__ = (db.PrimaryKeyConstraint("id", name="task_pkey"),)
    id = db.Column(UUID)
    created_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    updated_at = db.Column(
        db.DateTime, nullable=False, server_default=db.text("CURRENT_TIMESTAMP(0)")
    )
    knowledge_base_id = db.Column(UUID)
    status = db.Column(String)
    progress = db.Column(DECIMAL)
    latest_message = db.Column(String)

    def serialize(self):
        return {
            "id": self.id,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "knowledgeBaseId": self.knowledge_base_id,
            "latestMessage": self.latest_message,
            "status": self.status,
            "progress": self.progress,
        }

    @staticmethod
    def find_by_knowledge_base_id(knowledge_base_id: str):
        return (
            TaskEntity.query.filter_by(knowledge_base_id=knowledge_base_id)
            .order_by(TaskEntity.created_at.desc())
            .all()
        )

    @staticmethod
    def get_by_id(id: str):
        return TaskEntity.query.filter_by(id=id).first()

    @staticmethod
    def update_progress_by_id(
        task_id: str,
        status: TaskStatus,
        latest_message: str,
        progress: int = None,
    ):
        task = TaskEntity.query.filter_by(id=task_id).first()
        task.status = status.value
        if progress:
            task.progress = progress
        task.latest_message = latest_message
        db.session.commit()
        return task
