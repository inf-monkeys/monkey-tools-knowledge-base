import logging
from typing import Any
from uuid import uuid4

from loguru import logger
from pydantic import BaseModel
from pymilvus import MilvusClient, MilvusException, connections

from core.middleware.redis_client import redis_client
from core.models.document import Document
from core.models.field import Field
from core.storage.vectorstore.vector_store_base import BaseVectorStore


class MilvusConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    secure: bool = False
    batch_size: int = 100

    def validate_config(cls, values: dict) -> dict:
        if not values["host"]:
            raise ValueError("config vector.milvus.host is required")
        if not values["port"]:
            raise ValueError("config vector.milvus.port is required")
        if not values["user"]:
            raise ValueError("config vector.milvus.user is required")
        if not values["password"]:
            raise ValueError("config vector.milvus.password is required")
        return values

    def to_milvus_params(self):
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "secure": self.secure,
        }


class MilvusVector(BaseVectorStore):

    def __init__(self, collection_name: str, config: MilvusConfig):
        super().__init__(collection_name)
        self._client_config = config
        self._client = self._init_client(config)
        self._consistency_level = "Session"
        self._fields = []

    def get_type(self) -> str:
        return "milvus"

    def create(self, texts: list[Document], embeddings: list[list[float]], **kwargs):
        index_params = {
            "metric_type": "IP",
            "index_type": "HNSW",
            "params": {"M": 8, "efConstruction": 64},
        }
        metadatas = [d.metadata for d in texts]
        self.create_collection(embeddings, metadatas, index_params)
        self.add_texts(texts, embeddings)

    def update_by_id(self, id: str, document: Document) -> None:
        pass

    def add_texts(self, texts: list[Document], embeddings: list[list[float]], **kwargs):
        insert_dict_list = []
        for i in range(len(texts)):
            insert_dict = {
                Field.CONTENT_KEY.value: texts[i].page_content,
                Field.VECTOR.value: embeddings[i],
                Field.METADATA_KEY.value: texts[i].metadata,
            }
            insert_dict_list.append(insert_dict)
        # Total insert count
        total_count = len(insert_dict_list)

        pks: list[str] = []

        for i in range(0, total_count, 1000):
            batch_insert_list = insert_dict_list[i : i + 1000]
            # Insert into the collection.
            try:
                ids = self._client.insert(
                    collection_name=self._collection_name, data=batch_insert_list
                )
                pks.extend(ids)
            except MilvusException as e:
                logger.error(
                    "Failed to insert batch starting at entity: %s/%s", i, total_count
                )
                raise e
        return pks

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

        ids = self.get_ids_by_metadata_field(key, value)
        if ids:
            self._client.delete(collection_name=self._collection_name, pks=ids)

    def delete_by_ids(self, doc_ids: list[str]) -> None:
        self._client.delete(collection_name=self._collection_name, pks=doc_ids)

    def delete(self) -> None:
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

        if utility.has_collection(self._collection_name, using=alias):
            utility.drop_collection(self._collection_name, None, using=alias)

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

        # Set search parameters.
        results = self._client.search(
            collection_name=self._collection_name,
            data=[query_vector],
            limit=kwargs.get("top_k", 4),
            output_fields=[Field.CONTENT_KEY.value, Field.METADATA_KEY.value],
        )
        # Organize results.
        docs = []
        for result in results[0]:
            metadata = result["entity"].get(Field.METADATA_KEY.value)
            metadata["score"] = result["distance"]
            score_threshold = (
                kwargs.get("score_threshold") if kwargs.get("score_threshold") else 0.0
            )
            if result["distance"] > score_threshold:
                doc = Document(
                    page_content=result["entity"].get(Field.CONTENT_KEY.value),
                    metadata=metadata,
                    pk=str(result["id"]),
                )
                docs.append(doc)
        return docs

    def search_by_full_text(self, query: str, **kwargs: Any) -> list[Document]:
        # milvus/zilliz doesn't support bm25 search
        return []

    def create_collection(self, **kwargs):
        dimension = kwargs.get("dimension")
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

                # Determine embedding dim
                fields = []
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
                        Field.PRIMARY_KEY.value, DataType.INT64, is_primary=True, auto_id=True
                    )
                )
                # Create the vector field, supports binary or float vectors
                fields.append(
                    FieldSchema(
                        Field.VECTOR.value, DataType.FLOAT_VECTOR, dim=dimension
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
                    index_param={
                        "metric_type": "IP",
                        "index_type": "HNSW",
                        "params": {"M": 8, "efConstruction": 64},
                    },
                    consistency_level=self._consistency_level,
                )
            redis_client.set(collection_exist_cache_key, 1, ex=3600)

    def _init_client(self, config: MilvusConfig) -> MilvusClient:
        if config.secure:
            uri = "https://" + str(config.host) + ":" + str(config.port)
        else:
            uri = "http://" + str(config.host) + ":" + str(config.port)
        client = MilvusClient(uri=uri, user=config.user, password=config.password)
        return client

    def get_metadata_key_unique_values(self, key: str) -> list[str]:
        return []
