from flask import request, jsonify
from flask_restx import Resource
from core.config import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE, DEFAULT_SEPARATOR
from core.middleware.db import db
from core.models.knowledge_base import KnowledgeBaseEntity
from core.models.task import TaskEntity, TaskStatus
from core.models.document import DocumentEntity
import uuid

from core.queue.pub import submit_task
from core.queue.queue_name import QUEUE_NAME_PROCESS_FILE
from core.storage.vectorstore.vector_store_factory import VectorStoreFactory
from core.utils.zip import extract_files_from_zip


def register(api):
    knowledge_base_ns = api.namespace(
        "knowledge-bases", description="Knowledge Bases operations"
    )

    @knowledge_base_ns.route("/<string:knowledge_base_id>/documents")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_id", "The knowledge base identifier")
    class KnowledgeBaseDocuments(Resource):
        def get(self, knowledge_base_id):
            """List all documents in the knowledge base"""
            db.handle_invalid_transaction()
            KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            documents = DocumentEntity.find_by_knowledge_base_id(knowledge_base_id)
            return jsonify({"list": [document.serialize() for document in documents]})

        def post(self, knowledge_base_id):
            user_id = request.user_id
            """Create a new document in the knowledge base"""
            db.handle_invalid_transaction()
            KnowledgeBaseEntity.get_by_id(knowledge_base_id)

            data = request.json
            file_url = data.get("fileURL")
            filename = data.get("fileName")
            oss_type = data.get("ossType")
            oss_config = data.get("ossConfig", {})

            if not (file_url and filename) and not (oss_type and oss_config):
                raise ValueError(
                    "fileURL and fileName or ossType and ossConfig are required"
                )

            splitter_type = data.get("splitterType")
            pre_process_rules = data.get("preProcessRules", [])
            jqSchema = data.get("jqSchema", {})
            if splitter_type == "auto-segment":
                chunk_overlap = DEFAULT_CHUNK_OVERLAP
                chunk_size = DEFAULT_CHUNK_SIZE
                separator = DEFAULT_SEPARATOR
            elif splitter_type == "custom-segment":
                splitter_config = data.get("splitterConfig", {})
                chunk_overlap = splitter_config.get(
                    "chunk_overlap", DEFAULT_CHUNK_OVERLAP
                )
                chunk_size = splitter_config.get("chunk_size", DEFAULT_CHUNK_SIZE)
                separator = splitter_config.get("separator", DEFAULT_SEPARATOR)
            else:
                chunk_overlap = DEFAULT_CHUNK_OVERLAP
                chunk_size = DEFAULT_CHUNK_SIZE
                separator = DEFAULT_SEPARATOR

            # Save task to database
            task_id = str(uuid.uuid4())
            task_entity = TaskEntity(
                id=task_id,
                knowledge_base_id=knowledge_base_id,
                status=TaskStatus.PENDING.value,
                progress=0,
                latest_message="Added to queue",
            )
            db.session.add(task_entity)

            try:
                # Commit changes
                db.session.commit()
            except Exception:
                db.session.rollback()

            # Submit task to queue
            submit_task(
                QUEUE_NAME_PROCESS_FILE,
                {
                    "knowledge_base_id": knowledge_base_id,
                    "file_url": file_url,
                    "user_id": user_id,
                    "filename": filename,
                    "oss_type": oss_type,
                    "oss_config": oss_config,
                    "task_id": task_id,
                    "chunk_size": chunk_size,
                    "chunk_overlap": chunk_overlap,
                    "separator": separator,
                    "pre_process_rules": pre_process_rules,
                    "jqSchema": jqSchema,
                },
            )

            return {"task_id": task_id}

    @knowledge_base_ns.route(
        "/<string:knowledge_base_id>/documents/<string:document_id>"
    )
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("document_id", "The document identifier")
    class KnowledgeBaseDocumentDetail(Resource):
        def delete(self, knowledge_base_id, document_id):
            """Delete a document in the knowledge base"""
            db.handle_invalid_transaction()
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)

            vector_store = VectorStoreFactory(knowledge_base)
            vector_store.delete_by_metadata_field("document_id", document_id)
            DocumentEntity.delete_by_id(document_id)

            return {"success": True}
