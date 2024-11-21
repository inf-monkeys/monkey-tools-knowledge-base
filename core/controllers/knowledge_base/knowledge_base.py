from loguru import logger
from core.middleware.db import db
from flask import request, jsonify
from flask_restx import Resource
from core.utils.embedding import (
    get_dimension_by_embedding_model,
)
from core.models.knowledge_base import KnowledgeBaseEntity
import uuid
from core.storage.vectorstore.vector_store_factory import VectorStoreFactory


def register(api):
    knowledge_base_ns = api.namespace(
        "knowledge-bases", description="Knowledge Bases operations"
    )

    @knowledge_base_ns.route("/")
    class KnowledgeBaseList(Resource):
        """Create Knowledge Base"""

        @knowledge_base_ns.doc("create_knowledge_base")
        def post(self):
            """Create a new Collection"""
            # db.handle_invalid_transaction()
            data = request.json
            embedding_model = data.get("embeddingModel")
            dimension = get_dimension_by_embedding_model(embedding_model)

            knowledge_base_entity = KnowledgeBaseEntity(
                id=str(uuid.uuid4()),
                embedding_model=embedding_model,
                dimension=dimension,
            )
            db.session.add(knowledge_base_entity)
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()

            # Init vector collection if needed
            vector_store = VectorStoreFactory(knowledgebase=knowledge_base_entity)
            vector_store.create_collection(dimension=dimension)

            return jsonify(knowledge_base_entity.serialize())

    @knowledge_base_ns.route("/<string:knowledge_base_id>")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    class KnowledgeBaseDetail(Resource):
        """Manage Knowledge Base"""

        @knowledge_base_ns.doc("delete_knowledge_base")
        @knowledge_base_ns.response(204, "Knowledge base deleted")
        def delete(self, knowledge_base_id):
            """Delete a knowledge base given its identifier"""
            # db.handle_invalid_transaction()
            knowledge_base_entity = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(knowledgebase=knowledge_base_entity)
            try:
                vector_store.delete()
                KnowledgeBaseEntity.delete_by_id(knowledge_base_id)
            except Exception as e:
                logger.warning(f"Failed to delete vector store: {e}")
            return {"success": True}

    @knowledge_base_ns.route("/<string:knowledge_base_id>/copy")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    class KnowledgeBaseCopy(Resource):
        """Copy a Knowledge Base"""

        @knowledge_base_ns.doc("copy_knowledge_base")
        def post(self, knowledge_base_id):
            """Copy a knowledge base given its identifier"""
            # db.handle_invalid_transaction()
            pass
