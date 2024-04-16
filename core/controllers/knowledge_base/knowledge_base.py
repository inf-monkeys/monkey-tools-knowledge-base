from core.middleware.db import db
from flask import request, jsonify
from flask_restx import Resource
from core.es import ESClient
from core.utils import (
    generate_short_id,
)
from core.utils.embedding import (
    get_dimension_by_embedding_model,
    SUPPORTED_EMBEDDING_MODELS,
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
        @knowledge_base_ns.vendor(
            {
                "x-monkey-tool-name": "create_knowledge_base",
                "x-monkey-tool-categories": ["query", "db"],
                "x-monkey-tool-display-name": "创建知识库",
                "x-monkey-tool-description": "创建知识库",
                "x-monkey-tool-icon": "emoji:💿:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": "名称",
                        "name": "displayName",
                        "type": "string",
                        "required": True,
                    },
                    {
                        "displayName": "图标",
                        "name": "iconUrl",
                        "type": "string",
                        "required": False,
                    },
                    {
                        "displayName": "描述信息",
                        "name": "description",
                        "type": "string",
                        "required": False,
                    },
                    {
                        "displayName": "Embedding 模型",
                        "name": "embeddingModel",
                        "type": "options",
                        "options": [
                            {"name": item.get("name"), "value": item.get("name")}
                            for item in SUPPORTED_EMBEDDING_MODELS
                        ],
                    },
                ],
                "x-monkey-tool-output": [
                    {
                        "name": "name",
                        "displayName": "知识库唯一标志",
                        "type": "string",
                    },
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 5,
                },
            }
        )
        def post(self):
            """Create a new Collection"""
            data = request.json
            embedding_model = data.get("embeddingModel")
            dimension = get_dimension_by_embedding_model(embedding_model)

            knowledge_base_entity = KnowledgeBaseEntity(
                id=str(uuid.uuid4()),
                embedding_model=embedding_model,
                dimension=dimension,
            )
            db.session.add(knowledge_base_entity)
            db.session.commit()

            # Init vector collection if needed
            vector_store = VectorStoreFactory(knowledgebase=knowledge_base_entity)
            vector_store.init_collection(dimension=dimension)

            return jsonify(knowledge_base_entity.serialize())

    @knowledge_base_ns.route("/<string:knowledge_base_id>")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    class KnowledgeBaseDetail(Resource):
        """Manage Knowledge Base"""

        @knowledge_base_ns.doc("get_knowledge_base")
        def get(self, knowledge_base_name):
            """Fetch a given knowledge base"""
            app_id = request.app_id
            collection = get_knowledge_base_or_fail(app_id, knowledge_base_name)
            return jsonify(collection.serialize())

        @knowledge_base_ns.doc("delete_knowledge_base")
        @knowledge_base_ns.response(204, "Knowledge base deleted")
        def delete(self, knowledge_base_name):
            """Delete a knowledge base given its identifier"""
            team_id = request.team_id
            app_id = request.app_id
            model = get_knowledge_base_table_by_prefix(app_id)
            session.query(model).filter_by(
                team_id=team_id, name=knowledge_base_name
            ).update({"is_deleted": True})
            session.commit()
            es_client = ESClient(app_id=app_id, index_name=knowledge_base_name)
            es_client.delete_index()
            return {"success": True}

        @knowledge_base_ns.doc("update_knowledge_base")
        @knowledge_base_ns.response(201, "Knowledge base updated")
        def put(self, knowledge_base_name):
            """Update a knowledge base given its identifier"""
            team_id = request.team_id
            app_id = request.app_id
            model = get_knowledge_base_table_by_prefix(app_id)
            collection = get_knowledge_base_or_fail(
                app_id, team_id, knowledge_base_name
            )
            data = request.json
            description = data.get("description")
            display_name = data.get("displayName")
            icon_url = data.get("iconUrl")
            session.query(model).filter_by(
                name=knowledge_base_name, is_deleted=False
            ).update(
                {
                    "description": description or collection.description,
                    "display_name": display_name or collection.display_name,
                    "icon_url": icon_url or collection.icon_url,
                }
            )
            return {"success": True}

    @knowledge_base_ns.route("/<string:knowledge_base_id>/copy")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    class KnowledgeBaseCopy(Resource):
        """Copy a Knowledge Base"""

        @knowledge_base_ns.doc("copy_knowledge_base")
        def post(self, knowledge_base_name):
            """Copy a collection"""
            app_id = request.app_id
            team_id = request.team_id
            collection = get_knowledge_base_or_fail(
                app_id, team_id, knowledge_base_name
            )
            data = request.json
            team_id = data.get("teamId")
            user_id = data.get("userId")

            embedding_model = collection.embedding_model
            dimension = collection.dimension
            new_collection_name = generate_short_id()
            description = collection.description

            # 在 es 中创建 template
            es_client = ESClient(app_id=app_id, index_name=new_collection_name)
            es_client.create_es_index(dimension)
            model = get_knowledge_base_table_by_prefix(app_id)
            collection_entity = model(
                id=generate_mongoid(),
                creator_userId=user_id,
                team_id=team_id,
                name=new_collection_name,
                display_name=collection.display_name,
                description=description,
                icon_url=collection.icon_url,
                embedding_model=embedding_model,
                dimension=dimension,
                metadata_fields=collection.metadata_fields,
            )
            session.add(collection_entity)
            session.commit()
            return {"name": new_collection_name}
