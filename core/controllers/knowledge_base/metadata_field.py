from flask import jsonify
from flask_restx import Resource
from core.models.knowledge_base import KnowledgeBaseEntity
from core.models.metadata_field import MetadataFieldEntity, built_in_fields
from core.storage.vectorstore.vector_store_factory import VectorStoreFactory


def register(api):
    knowledge_base_ns = api.namespace(
        "knowledge-bases", description="Knowledge Bases operations"
    )

    @knowledge_base_ns.route("/<string:knowledge_base_id>/metadata-fields")
    class MetadataFieldList(Resource):
        def get(self, knowledge_base_id):
            """List Metadata Fields"""
            created_fields = [
                {
                    "displayName": field.key,
                    "name": field.key,
                }
                for field in MetadataFieldEntity.find_by_knowledge_base_id(
                    knowledge_base_id
                )
            ]
            return jsonify(built_in_fields + created_fields)

    @knowledge_base_ns.route(
        "/<string:knowledge_base_id>/metadata-fields/<string:field_key>/values"
    )
    class MetadataFieldValues(Resource):
        def get(self, knowledge_base_id, field_key):
            """List Metadata Field Values"""
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(knowledgebase=knowledge_base)
            values = vector_store.get_metadata_key_unique_values(field_key)
            return jsonify({"list": values})
