import time
from typing import List
from flask import request
from flask_restx import Resource
from core.es import ESClient
from core.models.document import Document
from core.models.metadata_field import MetadataFieldEntity
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
        """Create A Segment"""

        @knowledge_base_ns.doc("create_segment")
        def post(self, knowledge_base_id):
            """Create A Vector"""
            user_id = request.user_id
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            data = request.json
            text = data.get("text")
            if not text:
                raise Exception("text is empty")
            metadata = data.get("metadata", {})

            # set default metadata: created_at and user_id
            metadata["created_at"] = int(time.time())
            metadata["user_id"] = user_id

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
            vector_store.add_texts(text_list)

            metadata_fields = metadata.keys()
            MetadataFieldEntity.add_keys_if_not_exists(
                knowledge_base_id, metadata_fields
            )

            return {
                "inserted": len(text_list),
            }

    @knowledge_base_ns.route("/<string:knowledge_base_id>/segments/<string:pk>")
    @knowledge_base_ns.param("knowledge_base_id", "The knowledge base identifier")
    @knowledge_base_ns.param("pk", "The segment identifier")
    class SegmentDetail(Resource):
        """Create A Segment"""

        @knowledge_base_ns.doc("delete_segment")
        def delete(self, knowledge_base_id, pk):
            """Delete A Segment"""
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(
                knowledgebase=knowledge_base,
            )
            vector_store.delete_by_ids([pk])
            return {"success": True}

        @knowledge_base_ns.doc("upsert_segment")
        def put(self, knowledge_base_id, pk):
            """Update A Segment"""
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(
                knowledgebase=knowledge_base,
            )
            data = request.json
            text = data.get("text")
            metadata = data.get("metadata", {})
            vector_store.update_by_id(
                pk, Document(page_content=text, metadata=metadata)
            )
            metadata_fields = metadata.keys()
            MetadataFieldEntity.add_keys_if_not_exists(
                knowledge_base_id, metadata_fields
            )
            return {"success": True}
