from flask import request, jsonify
from flask_restx import Resource
from core.utils import ROOT_FOLDER
from core.utils.embedding import SUPPORTED_EMBEDDING_MODELS
from core.utils.oss.aliyunoss import AliyunOSSClient
from core.utils.oss.tos import TOSClient
import os
from FlagEmbedding import FlagReranker


def register(api):
    helpers_ns = api.namespace("helpers", description="Helpers")

    @helpers_ns.route("/embedding-models")
    class EmbeddingModels(Resource):
        """Shows a list of all todos, and lets you POST to add new tasks"""

        @helpers_ns.doc("get_embedding_models")
        def get(self):
            """List all supported embedding models"""
            return jsonify({"data": SUPPORTED_EMBEDDING_MODELS})

    @helpers_ns.route("/oss-connection")
    class OssConnection(Resource):
        """Shows a list of all todos, and lets you POST to add new tasks"""

        @helpers_ns.doc("test_oss_connections")
        def post(self):
            """Test OSS Connections"""
            data = request.json
            oss_type, oss_config = data.get("ossType"), data.get("ossConfig")
            if oss_type == "TOS":
                (
                    endpoint,
                    region,
                    bucket_name,
                    accessKeyId,
                    accessKeySecret,
                    baseFolder,
                ) = (
                    oss_config.get("endpoint"),
                    oss_config.get("region"),
                    oss_config.get("bucketName"),
                    oss_config.get("accessKeyId"),
                    oss_config.get("accessKeySecret"),
                    oss_config.get("baseFolder"),
                )
                tos_client = TOSClient(
                    endpoint,
                    region,
                    bucket_name,
                    accessKeyId,
                    accessKeySecret,
                )
                result = tos_client.test_connection(
                    baseFolder,
                )
                return {"result": result}
            elif oss_type == "ALIYUNOSS":
                endpoint, bucket_name, accessKeyId, accessKeySecret, baseFolder = (
                    oss_config.get("endpoint"),
                    oss_config.get("bucketName"),
                    oss_config.get("accessKeyId"),
                    oss_config.get("accessKeySecret"),
                    oss_config.get("baseFolder"),
                )
                aliyunoss_client = AliyunOSSClient(
                    endpoint=endpoint,
                    bucket_name=bucket_name,
                    access_key=accessKeyId,
                    secret_key=accessKeySecret,
                )
                result = aliyunoss_client.test_connection(
                    baseFolder,
                )
                return {"result": result}
            else:
                raise Exception(f"不支持的 oss 类型: {oss_type}")

    @helpers_ns.route("/reranker")
    class Reranker(Resource):
        """Shows a list of all todos, and lets you POST to add new tasks"""

        @helpers_ns.doc("reranker")
        @helpers_ns.vendor(
            {
                "x-monkey-tool-name": "reranker",
                "x-monkey-tool-categories": ["query"],
                "x-monkey-tool-display-name": {
                    "zh-CN": "文本相似度重排序",
                    "en-US": "Reranker",
                },
                "x-monkey-tool-description": {
                    "zh-CN": "基于 BAAI/bge-reranker-large 模型对文本进行相似度重排序",
                    "en-US": "Rerank text based on BAAI/bge-reranker-large model",
                },
                "x-monkey-tool-icon": "emoji:💿:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": "Query",
                        "name": "query",
                        "type": "string",
                        "default": "",
                        "required": True,
                    },
                    {
                        "displayName": {"zh-CN": "文档列表", "en-US": "Document List"},
                        "name": "array",
                        "type": "string",
                        "required": True,
                        "typeOptions": {
                            "multipleValues": True,
                        }
                    },
                    {
                        "displayName": {"zh-CN": "Top-K", "en-US": "Top-K"},
                        "description": {
                            "zh-CN": "返回 Top-K 个结果",
                            "en-US": "Return Top-K results",
                        },
                        "name": "topK",
                        "type": "number",
                    },
                ],
                "x-monkey-tool-output": [
                    {
                        "name": "sortedArray",
                        "displayName": {
                            "zh-CN": "重排序后的文档列表",
                            "en-US": "Reranked Document List",
                        },
                        "type": "collection",
                    },
                    {
                        "name": "scores",
                        "displayName": {
                            "zh-CN": "文档列表的分数",
                            "en-US": "Scores of Document List",
                        },
                        "type": "string",
                    },
                    {
                        "name": "str",
                        "displayName": {
                            "zh-CN": "重排序后的文档列表（字符串）",
                            "en-US": "Reranked Document List (String)",
                        },
                        "type": "string",
                    },
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 5,
                },
            }
        )
        def post(self):
            """Reranker"""
            input_data = request.json
            query = input_data.get("query")
            array = input_data.get("array")
            top_k = input_data.get("topK")

            model_or_path = (
                os.path.join(ROOT_FOLDER, "models/bge-reranker-large")
                if os.path.exists(
                    os.path.join(ROOT_FOLDER, "models/bge-reranker-large")
                )
                else "BAAI/bge-reranker-large"
            )
            reranker = FlagReranker(model_or_path, use_fp16=True)
            args = [[query, item] for item in array]

            scores = reranker.compute_score(args)
            sorted_array = [
                item for score, item in sorted(zip(scores, array), reverse=True)
            ]

            if top_k != None:
                sorted_array = sorted_array[:top_k]
            return {
                "scores": scores,
                "sortedArray": sorted_array,
                "str": "\n".join(sorted_array),
            }

    @helpers_ns.route("/text-to-embedding")
    class TextToEmbedding(Resource):
        """Shows a list of all todos, and lets you POST to add new tasks"""

        @helpers_ns.doc("text_to_embedding")
        @helpers_ns.vendor(
            {
                "x-monkey-tool-name": "text_to_embedding",
                "x-monkey-tool-categories": ["query", "text"],
                "x-monkey-tool-display-name": {
                    "zh-CN": "文本转向量",
                    "en-US": "Text to Embedding",
                },
                "x-monkey-tool-description": {
                    "zh-CN": "将文本转换为向量",
                    "en-US": "Convert text to embedding",
                },
                "x-monkey-tool-icon": "emoji:💿:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": {
                            "zh-CN": "文本",
                            "en-US": "Text",
                        },
                        "name": "text",
                        "type": "string",
                        "default": "",
                        "required": True,
                    },
                    {
                        "displayName": {
                            "zh-CN": "Embedding Model",
                            "en-US": "Embedding Model",
                        },
                        "name": "embeddingModel",
                        "type": "options",
                        "options": [
                            {
                                "name": item.get("displayName"),
                                "value": item.get("name"),
                                "disabled": not item.get("enabled"),
                            }
                            for item in SUPPORTED_EMBEDDING_MODELS
                        ],
                        "default": SUPPORTED_EMBEDDING_MODELS[0].get("name"),
                        "required": True,
                    },
                ],
                "x-monkey-tool-output": [
                    {
                        "name": "vectorArray",
                        "displayName": {
                            "zh-CN": "向量",
                            "en-US": "Vector",
                        },
                        "type": "collection",
                    }
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 3,
                },
            }
        )
        def post(self):
            """Reranker"""
            input_data = request.json
            query = input_data.get("query")
            array = input_data.get("array")
            top_k = input_data.get("topK")

            model_or_path = (
                os.path.join(ROOT_FOLDER, "models/bge-reranker-large")
                if os.path.exists(
                    os.path.join(ROOT_FOLDER, "models/bge-reranker-large")
                )
                else "BAAI/bge-reranker-large"
            )
            reranker = FlagReranker(model_or_path, use_fp16=True)
            args = [[query, item] for item in array]

            scores = reranker.compute_score(args)
            sorted_array = [
                item for score, item in sorted(zip(scores, array), reverse=True)
            ]

            if top_k != None:
                sorted_array = sorted_array[:top_k]
            return {
                "scores": scores,
                "sortedArray": sorted_array,
                "str": "\n".join(sorted_array),
            }
