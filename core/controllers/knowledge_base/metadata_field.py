from flask import jsonify
from flask_restx import Resource
from core.models.metadata_field import MetadataFieldEntity, built_in_fields


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
