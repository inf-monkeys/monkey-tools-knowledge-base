import logging
import traceback
from typing import Any, Optional
from uuid import uuid4

import elasticsearch
from pydantic import BaseModel
from core.models.field import Field
from core.models.document import Document
from core.storage.vectorstore.vector_store_base import BaseVectorStore
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

from core.utils import chunk_list, generate_md5

logger = logging.getLogger(__name__)


class ElasticSearchConfig(BaseModel):
    url: str
    username: str
    password: str
    knn_num_candidates: int = 100
    secure: bool = False
    batch_size: int = 100

    def validate_config(cls, values: dict) -> dict:
        if not values["url"]:
            raise ValueError("config vector.elasticsearch.url is required")
        if not values["username"]:
            raise ValueError("config vector.elasticsearch.username is required")
        if not values["password"]:
            raise ValueError("config vector.elasticsearch.url password required")
        return values

    def to_elasticsearch_params(self):
        return {
            "url": self.url,
            "username": self.username,
            "password": self.password,
            "knn_num_candidates": self.knn_num_candidates,
            "batch_size": self.batch_size,
        }


class ElasticsearchVectorStore(BaseVectorStore):
    def __init__(self, collection_name: str, config: ElasticSearchConfig):
        super().__init__(collection_name)
        self._client_config = config
        self._client = self._init_client(config)
        self._consistency_level = "Session"
        self._fields = []

    def init_collection(self, **kwargs) -> BaseVectorStore:
        dimension = kwargs.get("dimension")
        self._client.indices.create(
            index=self._collection_name,
            mappings={
                "properties": {
                    "page_content": {"type": "text"},
                    "embeddings": {
                        "type": "dense_vector",
                        "dims": dimension,
                        "similarity": "l2_norm",
                    },
                    "metadata": {
                        "type": "object",
                        "properties": {
                            "created_at": {"type": "date"},
                            "filename": {"type": "keyword"},
                            "document_id": {"type": "keyword"},
                        },
                    },
                }
            },
        )

    def get_type(self) -> str:
        return "elasticsearch"

    def __upsert_documents_batch(self, all_documents):
        # 准备批量数据
        chunks = chunk_list(all_documents, self._client_config.batch_size)
        for chunk in chunks:
            try:
                helpers.bulk(self._client, chunk)
            except BulkIndexError as e:
                print(f"An error occurred: {e}")
                for i, error in enumerate(e.errors):
                    # 输出每个失败文档的详细错误信息
                    print(f"Document {i} failed: {error}")
                raise e
            except elasticsearch.ConnectionError as e:
                traceback.print_exc()
                raise Exception("Elasticsearch connection error")

    def save_documents(
        self,
        texts: list[Document],
        embeddings: list[list[float]],
        **kwargs,
    ):
        es_documents = []
        for index, item in enumerate(texts):
            es_documents.append(
                {
                    "_index": self._collection_name,
                    "_id": generate_md5(item.page_content),
                    "_source": {
                        "page_content": item.page_content,
                        "metadata": item.metadata,
                        "embeddings": embeddings[index],
                    },
                }
            )
        self.__upsert_documents_batch(es_documents)

    def delete_by_document_id(self, document_id: str):

        ids = self.get_ids_by_metadata_field("document_id", document_id)
        if ids:
            self._client.delete(collection_name=self._collection_name, pks=ids)

    def get_ids_by_metadata_field(self, key: str, value: str):
        result = self._client.query(
            collection_name=self._collection_name,
            filter=f'metadata["{key}"] == "{value}"',
            output_fields=["id"],
        )
        if result:
            return [item["id"] for item in result]
        else:
            return None

    def delete_by_metadata_field(self, key: str, value: str):
        # Delete by metadata field
        res = self._client.delete_by_query(
            index=self._collection_name,
            body={"query": {"term": {f"metadata.{key}": value}}},
        )
        logger.info(f"Deleted {res['deleted']} documents")

    def delete_by_ids(self, doc_ids: list[str]) -> None:

        result = self._client.query(
            collection_name=self._collection_name,
            filter=f'metadata["doc_id"] in {doc_ids}',
            output_fields=["id"],
        )
        if result:
            ids = [item["id"] for item in result]
            self._client.delete(collection_name=self._collection_name, pks=ids)

    def delete(self) -> None:
        # delete the entire index
        self._client.indices.delete(index=self._collection_name)

    def text_exists(self, id: str) -> bool:
        alias = uuid4().hex
        if self._client_config.secure:
            uri = (
                "https://"
                + str(self._client_config.host)
                + ":"
                + str(self._client_config.port)
            )
        else:
            uri = (
                "http://"
                + str(self._client_config.host)
                + ":"
                + str(self._client_config.port)
            )
        connections.connect(
            alias=alias,
            uri=uri,
            user=self._client_config.user,
            password=self._client_config.password,
        )

        from pymilvus import utility

        if not utility.has_collection(self._collection_name, using=alias):
            return False

        result = self._client.query(
            collection_name=self._collection_name,
            filter=f'metadata["doc_id"] == "{id}"',
            output_fields=["id"],
        )

        return len(result) > 0

    def search_by_vector(
        self, query_vector: list[float], **kwargs: Any
    ) -> list[Document]:
        must_statements = []
        metadata_filter = kwargs.get("metadata_filter", None)
        top_k = kwargs.get("top_k", 3)
        if metadata_filter:
            for key, value in metadata_filter.items():
                must_statements.append({"term": {f"metadata.{key}.keyword": value}})

        search_body = {
            "knn": {
                "field": "embeddings",
                "query_vector": query_vector,
                "k": top_k,
                "num_candidates": self._client_config.knn_num_candidates,
            },
            "fields": ["page_content", "metadata"],
        }
        if len(must_statements) > 0:
            search_body["query"] = {"bool": {"must": must_statements}}
        response = self._client.search(index=self._collection_name, body=search_body)

        return [
            Document(
                pk=hit["_id"],
                page_content=hit["_source"]["page_content"],
                metadata=hit["_source"]["metadata"],
            )
            for hit in response["hits"]["hits"]
        ]

    def search_by_full_text(self, query: str, **kwargs: Any) -> list[Document]:
        """Full Text Search
        :param query: 搜索关键词
        :param expr:
        :param metadata_filter:
        :param size:
        :return:
        """
        metadata_filter = kwargs.get("metadata_filter", None)
        from_ = kwargs.get("from_", 0)
        size = kwargs.get("size", 10)
        sort_by_created_at = kwargs.get("sort_by_created_at", False)

        must_statements = []
        if query:
            must_statements.append({"match": {"page_content": query}})

        if metadata_filter:
            for key, value in metadata_filter.items():
                if value is not None:
                    must_statements.append({"term": {f"metadata.{key}.keyword": value}})
        try:
            response = self._client.search(
                index=self._collection_name,
                query={"bool": {"must": must_statements}},
                from_=from_,
                size=size,
                sort=(
                    [{"metadata.created_at": {"order": "desc"}}]
                    if sort_by_created_at
                    else None
                ),
            )

            return [
                Document(
                    pk=hit["_id"],
                    page_content=hit["_source"]["page_content"],
                    metadata=hit["_source"]["metadata"],
                )
                for hit in response["hits"]["hits"]
            ]
        except elasticsearch.NotFoundError:
            return []

    def create_collection(
        self,
        embeddings: list,
        metadatas: Optional[list[dict]] = None,
        index_params: Optional[dict] = None,
    ):
        lock_name = "vector_indexing_lock_{}".format(self._collection_name)
        with redis_client.lock(lock_name, timeout=20):
            collection_exist_cache_key = "vector_indexing_{}".format(
                self._collection_name
            )
            if redis_client.get(collection_exist_cache_key):
                return
            # Grab the existing collection if it exists
            from pymilvus import utility

            alias = uuid4().hex
            if self._client_config.secure:
                uri = (
                    "https://"
                    + str(self._client_config.host)
                    + ":"
                    + str(self._client_config.port)
                )
            else:
                uri = (
                    "http://"
                    + str(self._client_config.host)
                    + ":"
                    + str(self._client_config.port)
                )
            connections.connect(
                alias=alias,
                uri=uri,
                user=self._client_config.user,
                password=self._client_config.password,
            )
            if not utility.has_collection(self._collection_name, using=alias):
                from pymilvus import CollectionSchema, DataType, FieldSchema
                from pymilvus.orm.types import infer_dtype_bydata

                # Determine embedding dim
                dim = len(embeddings[0])
                fields = []
                if metadatas:
                    fields.append(
                        FieldSchema(
                            Field.METADATA_KEY.value, DataType.JSON, max_length=65_535
                        )
                    )

                # Create the text field
                fields.append(
                    FieldSchema(
                        Field.CONTENT_KEY.value, DataType.VARCHAR, max_length=65_535
                    )
                )
                # Create the primary key field
                fields.append(
                    FieldSchema(
                        Field.PRIMARY_KEY.value,
                        DataType.INT64,
                        is_primary=True,
                        auto_id=True,
                    )
                )
                # Create the vector field, supports binary or float vectors
                fields.append(
                    FieldSchema(
                        Field.VECTOR.value, infer_dtype_bydata(embeddings[0]), dim=dim
                    )
                )

                # Create the schema for the collection
                schema = CollectionSchema(fields)

                for x in schema.fields:
                    self._fields.append(x.name)
                # Since primary field is auto-id, no need to track it
                self._fields.remove(Field.PRIMARY_KEY.value)

                # Create the collection
                collection_name = self._collection_name
                self._client.create_collection_with_schema(
                    collection_name=collection_name,
                    schema=schema,
                    index_param=index_params,
                    consistency_level=self._consistency_level,
                )
            redis_client.set(collection_exist_cache_key, 1, ex=3600)

    def _init_client(self, config: ElasticSearchConfig) -> Elasticsearch:
        return Elasticsearch(
            config.url,
            http_auth=(
                (config.username, config.password)
                if config.username and config.password
                else None
            ),
            verify_certs=False,
        )
