from flask import request, jsonify
from flask_restx import Resource
from core.utils.embedding import (
    generate_embedding_of_model,
)
from core.storage.vectorstore.vector_store_factory import VectorStoreFactory
from core.models.knowledge_base import KnowledgeBaseEntity


def register(api):
    knowledge_base_ns = api.namespace(
        "knowledge-bases", description="Knowledge Bases operations"
    )

    @knowledge_base_ns.route("/<string:knowledge_base_id>/fulltext-search")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    class KnowledgeBaseFullTextSearch(Resource):
        @knowledge_base_ns.doc("fulltext_search")
        @knowledge_base_ns.vendor(
            {
                "x-monkey-tool-name": "fulltext_search_documents",
                "x-monkey-tool-categories": ["query"],
                "x-monkey-tool-display-name": "文本全文搜索",
                "x-monkey-tool-description": "对文本进行全文关键字搜索，返回最匹配的文档列表",
                "x-monkey-tool-icon": "emoji:💿:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": "文本数据库",
                        "name": "knowledge_base_id",
                        "type": "string",
                        "typeOptions": {"assetType": "knowledge-base"},
                        "default": "",
                        "required": True,
                    },
                    {
                        "displayName": "关键词",
                        "name": "query",
                        "type": "string",
                        "default": "",
                        "required": False,
                    },
                    {
                        "displayName": "TopK",
                        "name": "topK",
                        "type": "number",
                        "default": 3,
                        "required": False,
                    },
                    {
                        "displayName": "数据过滤方式",
                        "name": "filterType",
                        "type": "options",
                        "options": [
                            {"name": "简单形式", "value": "simple"},
                            {"name": "ES 表达式", "value": "es-expression"},
                        ],
                        "default": "simple",
                        "required": False,
                    },
                    {
                        "displayName": "根据元数据的字段进行过滤",
                        "name": "metadata_filter",
                        "type": "json",
                        "typeOptions": {
                            "multiFieldObject": True,
                            "multipleValues": False,
                        },
                        "default": "",
                        "required": False,
                        "description": "根据元数据的字段进行过滤",
                        "displayOptions": {"show": {"filterType": ["simple"]}},
                    },
                    {
                        "name": "docs",
                        "type": "notice",
                        "displayName": """使用 ES 搜索过滤表达式用于对文本进行精准过滤。\n示例：
        ```json
        {
            "term": {
                "metadata.filename.keyword": "文件名称"
            }
        }
        ```
                    """,
                        "displayOptions": {"show": {"filterType": ["es-expression"]}},
                    },
                    {
                        "displayName": "过滤表达式",
                        "name": "expr",
                        "type": "json",
                        "required": False,
                        "displayOptions": {"show": {"filterType": ["es-expression"]}},
                    },
                    {
                        "displayName": "是否按照创建时间进行排序",
                        "name": "orderByCreatedAt",
                        "type": "boolean",
                        "required": False,
                        "default": False,
                    },
                ],
                "x-monkey-tool-output": [
                    {
                        "name": "hits",
                        "displayName": "相似性集合",
                        "type": "json",
                        "typeOptions": {
                            "multipleValues": True,
                        },
                        "properties": [
                            {
                                "name": "metadata",
                                "displayName": "元数据",
                                "type": "json",
                            },
                            {
                                "name": "page_content",
                                "displayName": "文本内容",
                                "type": "string",
                            },
                        ],
                    },
                    {
                        "name": "text",
                        "displayName": "所有搜索的结果组合的字符串",
                        "type": "string",
                    },
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 5,
                },
            }
        )
        def post(self, knowledge_base_id):
            """Index all terms in the document, allowing users to search any term and retrieve relevant text chunk containing those terms."""
            data = request.json
            query = data.get("query", None)
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(knowledgebase=knowledge_base)
            from_ = data.get("from", 0)
            size = data.get("size", 30)
            metadata_filter = data.get("metadataFilter", None)
            sort_by_created_at = data.get("sortByCreatedAt", False)
            documents = vector_store.search_by_full_text(
                query,
                metadata_filter=metadata_filter,
                from_=from_,
                size=size,
                sort_by_created_at=sort_by_created_at,
            )
            return {
                "hits": [document.serialize() for document in documents],
                "text": "\n\n".join([document.page_content for document in documents]),
            }

    @knowledge_base_ns.route("/<string:knowledge_base_id>/vector-search")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    class KnowledgeBaseVectorSearch(Resource):
        @knowledge_base_ns.doc("vector_search")
        @knowledge_base_ns.vendor(
            {
                "x-monkey-tool-name": "search_vector",
                "x-monkey-tool-categories": ["query"],
                "x-monkey-tool-display-name": "文本向量搜索",
                "x-monkey-tool-description": "根据提供的文本对进行相似性搜索",
                "x-monkey-tool-icon": "emoji:💿:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": "文本数据库",
                        "name": "knowledge_base_id",
                        "type": "string",
                        "typeOptions": {"assetType": "knowledge-base"},
                        "default": "",
                        "required": True,
                    },
                    {
                        "displayName": "关键词",
                        "name": "query",
                        "type": "string",
                        "default": "",
                        "required": True,
                    },
                    {
                        "displayName": "topK",
                        "name": "topK",
                        "type": "number",
                        "default": 3,
                        "required": False,
                    },
                    {
                        "displayName": "根据元数据字段进行过滤",
                        "name": "metadata_filter",
                        "type": "json",
                        "typeOptions": {
                            "multiFieldObject": True,
                            "multipleValues": False,
                        },
                        "default": "",
                        "required": False,
                        "description": "根据元数据的字段进行过滤",
                    },
                ],
                "x-monkey-tool-output": [
                    {
                        "name": "hits",
                        "displayName": "段落列表",
                        "type": "json",
                        "typeOptions": {
                            "multipleValues": True,
                        },
                        "properties": [
                            {
                                "name": "metadata",
                                "displayName": "元数据",
                                "type": "json",
                            },
                            {
                                "name": "page_content",
                                "displayName": "文本内容",
                                "type": "string",
                            },
                        ],
                    },
                    {
                        "name": "text",
                        "displayName": "所有搜索的结果组合的字符串",
                        "type": "string",
                    },
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 5,
                },
            }
        )
        def post(self, knowledge_base_id):
            """Generate query embeddings and search for the text chunk most similar to its vector representation."""
            input_data = request.json
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(knowledgebase=knowledge_base)
            query = input_data.get("query")
            if not query:
                raise Exception("query is empty")
            top_k = input_data.get("topK", 3)
            metadata_filter = input_data.get("metadata_filter", None)
            documents = vector_store.search_by_vector(
                query=query,
                metadata_filter=metadata_filter,
                top_k=top_k,
            )
            return {
                "hits": [document.serialize() for document in documents],
                "text": "\n\n".join([document.page_content for document in documents]),
            }

    # @knowledge_base_ns.route("/<string:knowledge_base_id>/hybird-search")
    # @knowledge_base_ns.response(404, "Knowledge base not found")
    # @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    # class KnowledgeBaseHybirdSearch(Resource):
    #     @knowledge_base_ns.doc("hybird_search")
    #     @knowledge_base_ns.vendor(
    #         {
    #             "x-monkey-tool-name": "hybird_search",
    #             "x-monkey-tool-categories": ["query"],
    #             "x-monkey-tool-display-name": "综合搜索",
    #             "x-monkey-tool-description": "根据提供的文本对进行相似性搜索",
    #             "x-monkey-tool-icon": "emoji:💿:#e58c3a",
    #             "x-monkey-tool-input": [
    #                 {
    #                     "displayName": "文本数据库",
    #                     "name": "knowledgeBaseName",
    #                     "type": "string",
    #                     "typeOptions": {"assetType": "knowledge-base"},
    #                     "default": "",
    #                     "required": True,
    #                 },
    #                 {
    #                     "displayName": "关键词",
    #                     "name": "question",
    #                     "type": "string",
    #                     "default": "",
    #                     "required": True,
    #                 },
    #                 {
    #                     "displayName": "TopK",
    #                     "name": "topK",
    #                     "type": "number",
    #                     "default": 3,
    #                     "required": False,
    #                 },
    #                 {
    #                     "displayName": "根据元数据字段进行过滤",
    #                     "name": "metadata_filter",
    #                     "type": "json",
    #                     "typeOptions": {
    #                         "multiFieldObject": True,
    #                         "multipleValues": False,
    #                     },
    #                     "default": "",
    #                     "required": False,
    #                     "description": "根据元数据的字段进行过滤",
    #                 },
    #             ],
    #             "x-monkey-tool-output": [
    #                 {
    #                     "name": "result",
    #                     "displayName": "相似性集合",
    #                     "type": "json",
    #                     "typeOptions": {
    #                         "multipleValues": True,
    #                     },
    #                     "properties": [
    #                         {
    #                             "name": "metadata",
    #                             "displayName": "元数据",
    #                             "type": "json",
    #                         },
    #                         {
    #                             "name": "page_content",
    #                             "displayName": "文本内容",
    #                             "type": "string",
    #                         },
    #                     ],
    #                 },
    #                 {
    #                     "name": "text",
    #                     "displayName": "所有搜索的结果组合的字符串",
    #                     "type": "string",
    #                 },
    #             ],
    #             "x-monkey-tool-extra": {
    #                 "estimateTime": 5,
    #             },
    #         }
    #     )
    #     def post(self, knowledgeBaseName):
    #         """Execute full-text search and vector searches simultaneously, re-rank to select the best match for the user's query. Configuration of the Rerank model APIis necessary."""
    #         input_data = request.json
    #         team_id = request.team_id
    #         query = input_data.get("query")
    #         if not query:
    #             raise Exception("query is empty")
    #         top_k = input_data.get("topK", 3)
    #         metadata_filter = input_data.get("metadata_filter", None)

    #         app_id = request.app_id
    #         collection = get_knowledge_base_or_fail(app_id, team_id, knowledgeBaseName)

    #         es_client = ESClient(app_id=app_id, index_name=knowledgeBaseName)
    #         embedding_model = collection.embedding_model
    #         embedding = generate_embedding_of_model(embedding_model, query)

    #         data = es_client.vector_search(embedding, top_k, metadata_filter)
    #         data = [
    #             {
    #                 "page_content": item["_source"]["page_content"],
    #                 "metadata": item["_source"]["metadata"],
    #             }
    #             for item in data
    #         ]
    #         texts = [item["page_content"] for item in data]
    #         text = "\n".join(texts)

    #         return {"result": data, "text": text}
