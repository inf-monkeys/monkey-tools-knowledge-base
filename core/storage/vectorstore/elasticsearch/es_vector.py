import logging
import traceback
from typing import Any
import elasticsearch
from pydantic import BaseModel
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
        
    def create_collection(self, **kwargs) -> BaseVectorStore:
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
                            "created_at": {
                                "type": "date",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                            "filename": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                            "document_id": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
                            "user_id": {
                                "type": "text",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                },
                            },
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

    def add_texts(
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
        # TODO: Delete by batch
        for pk in doc_ids:
            self._client.delete(index=self._collection_name, id=pk)

    def update_by_id(self, id: str, document: Document) -> None:
        self._client.update(
            index=self._collection_name,
            id=id,
            body={
                "doc": {
                    "page_content": document.page_content,
                    "metadata": document.metadata,
                }
            },
        )

    def delete(self) -> None:
        # delete the entire index
        self._client.indices.delete(index=self._collection_name)

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
            sort = (
                [{"metadata.created_at": {"order": "desc"}}]
                if sort_by_created_at
                else None
            )
            response = self._client.search(
                index=self._collection_name,
                query={"bool": {"must": must_statements}},
                from_=from_,
                size=size,
                sort=sort,
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

    def text_exists(self, id: str) -> bool:
        return self._client.exists(index=self._collection_name, id=id)

    def get_metadata_key_unique_values(self, key: str) -> list[str]:
        return []
