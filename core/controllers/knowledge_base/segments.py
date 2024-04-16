from typing import List
from flask import request, jsonify
from flask_restx import Resource
from core.es import ESClient
from core.models.document import Document
from core.utils import (
    generate_md5,
)
from core.utils.embedding import (
    generate_embedding_of_model,
)
from core.models.knowledge_base import KnowledgeBaseEntity
from core.storage.vectorstore.vector_store_factory import VectorStoreFactory


def register(api):
    knowledge_base_ns = api.namespace(
        "knowledge-bases", description="Knowledge Bases operations"
    )

    @knowledge_base_ns.route("/<string:knowledge_base_id>/segments")
    @knowledge_base_ns.param("knowledge_base_id", "The knowledge base identifier")
    class KnowledgeBaseSegments(Resource):
        """List Segments of a Document"""

        @knowledge_base_ns.doc("list_segments")
        def get(self, knowledge_base_name):
            """List all segments of a document"""
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_name)
            document = knowledge_base.get_document(document_id)
            return jsonify(document.segments)

        """Create A Segment"""

        @knowledge_base_ns.doc("create_segment")
        def post(self, knowledge_base_id):
            """Create A Vector"""
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            data = request.json
            text = data.get("text")
            if not text:
                raise Exception("text is empty")
            metadata = data.get("metadata", {})
            delimiter = data.get("delimiter")

            text_list: List[Document] = []
            if delimiter:
                delimiter = delimiter.replace("\\n", "\n")
                texts = text.split(delimiter)
                text_list = [
                    Document(
                        page_content=item,
                        metadata=metadata,
                    )
                    for item in texts
                ]
            else:
                text_list = [
                    Document(
                        page_content=text,
                        metadata=metadata,
                    )
                ]

            vector_store = VectorStoreFactory(
                knowledgebase=knowledge_base,
            )
            vector_store.save_documents(text_list)
            return {
                "inserted": len(text_list),
            }

    @knowledge_base_ns.route(
        "/<string:knowledge_base_id>/<string:document_id>/segments/<string:pk>"
    )
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    @knowledge_base_ns.param("document_id", "The document identifier")
    @knowledge_base_ns.param("pk", "The segment identifier")
    class SegmentDetail(Resource):
        """Create A Segment"""

        @knowledge_base_ns.doc("delete_segment")
        def delete(self, knowledge_base_name, document_id, pk):
            """Delete A Segment"""
            app_id = request.app_id
            es_client = ESClient(app_id=app_id, index_name=knowledge_base_name)
            es_client.delete_es_document(pk)
            return {"success": True}

        @knowledge_base_ns.doc("upsert_segment")
        def put(self, knowledge_base_name, document_id, pk):
            """Upsert A Segment"""
            data = request.json
            team_id = request.team_id
            app_id = request.app_id
            text = data.get("text")
            if not text:
                raise Exception("text is empty")
            metadata = data.get("metadata")
            collection = get_knowledge_base_or_fail(
                app_id, team_id, knowledge_base_name
            )
            embedding_model = collection.embedding_model
            embedding = generate_embedding_of_model(embedding_model, [text])
            es_client = ESClient(app_id=app_id, index_name=knowledge_base_name)
            result = es_client.upsert_document(
                pk=pk,
                document={
                    "page_content": text,
                    "metadata": metadata,
                    "embeddings": embedding[0],
                },
            )
            return {"success": True}

        @knowledge_base_ns.doc("get_segment")
        def get(self, knowledge_base_name, document_id, pk):
            """Get A Segment"""
            app_id = request.app_id
            es_client = ESClient(app_id=app_id, index_name=knowledge_base_name)
            result = es_client.get_document(pk)
            return result
