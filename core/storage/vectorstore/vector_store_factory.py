import json
from typing import Any
from flask import current_app
from core.models.document import Document
from core.models.knowledge_base import KnowledgeBaseEntity
from core.storage.vectorstore.vector_store_base import BaseVectorStore
from core.config import vector_config
from core.utils.embedding import generate_embedding_of_model

vector_type = vector_config.get("type")


class VectorStoreFactory:
    def __init__(self, knowledgebase: KnowledgeBaseEntity, attributes: list = None):
        if attributes is None:
            attributes = ["doc_id", "knowledgebase_id", "document_id", "doc_hash"]
        self._knowledgebase = knowledgebase
        self._attributes = attributes
        self._vector_processor = self._init_vector()

    def _init_vector(self) -> BaseVectorStore:
        if not vector_type:
            raise ValueError("Vector store must be specified.")

        if vector_type == "weaviate":
            from core.storage.vectorstore.weaviate.weaviate_vector import (
                WeaviateConfig,
                WeaviateVector,
            )

            weaviate_config = vector_config.get("weaviate")
            endpoint = weaviate_config.get("endpoint")
            api_key = weaviate_config.get("api_key")
            batch_size = weaviate_config.get("batch_size", 100)
            dataset_id = self._knowledgebase.id
            collection_name = KnowledgeBaseEntity.gen_collection_name_by_id(dataset_id)
            return WeaviateVector(
                collection_name=collection_name,
                config=WeaviateConfig(
                    endpoint,
                    api_key,
                    batch_size=batch_size,
                ),
                attributes=self._attributes,
            )
        elif vector_type == "qdrant":
            from core.storage.vectorstore.qdrant.qdrant_vector import (
                QdrantConfig,
                QdrantVector,
            )

            qdrant_config = vector_config.get("qdrant")
            endpoint = qdrant_config.get("endpoint")
            api_key = qdrant_config.get("api_key")
            timeout = qdrant_config.get("timeout", 20)
            dataset_id = self._knowledgebase.id
            collection_name = KnowledgeBaseEntity.gen_collection_name_by_id(dataset_id)
            return QdrantVector(
                collection_name=collection_name,
                group_id=self._knowledgebase.id,
                config=QdrantConfig(
                    endpoint,
                    api_key,
                    root_path=current_app.root_path,
                    timeout=timeout,
                ),
            )
        elif vector_type == "milvus":
            from core.storage.vectorstore.milvus.milvus_vector import (
                MilvusConfig,
                MilvusVector,
            )

            milvus_config = vector_config.get("milvus")
            host = milvus_config.get("host")
            port = milvus_config.get("port")
            user = milvus_config.get("user")
            password = milvus_config.get("password")
            secure = milvus_config.get("secure", False)
            dataset_id = self._knowledgebase.id
            collection_name = KnowledgeBaseEntity.gen_collection_name_by_id(dataset_id)
            return MilvusVector(
                collection_name=collection_name,
                config=MilvusConfig(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    secure=secure,
                ),
            )
        elif vector_type == "elasticsearch":
            from core.storage.vectorstore.elasticsearch.es_vector import (
                ElasticSearchConfig,
                ElasticsearchVectorStore,
            )

            es_config = vector_config.get("elasticsearch")
            url = es_config.get("url")
            username = es_config.get("username")
            password = es_config.get("password")
            dataset_id = self._knowledgebase.id
            collection_name = KnowledgeBaseEntity.gen_collection_name_by_id(dataset_id)
            return ElasticsearchVectorStore(
                collection_name=collection_name,
                config=ElasticSearchConfig(
                    url=url,
                    username=username,
                    password=password,
                ),
            )
        else:
            raise ValueError(f"Vector store {vector_type} is not supported.")

    def save_documents(self, documents: list[Document], **kwargs):
        if kwargs.get("duplicate_check", False):
            documents = self._filter_duplicate_texts(documents)
        embeddings = generate_embedding_of_model(
            self._knowledgebase.embedding_model,
            [document.page_content for document in documents],
        )
        self._vector_processor.save_documents(
            texts=documents, embeddings=embeddings, **kwargs
        )

    def text_exists(self, id: str) -> bool:
        return self._vector_processor.text_exists(id)

    def delete_by_ids(self, ids: list[str]) -> None:
        self._vector_processor.delete_by_ids(ids)
        
    def update_by_id(self, id: str, document: Document) -> None:
        self._vector_processor.update_by_id(id, document)

    def delete_by_metadata_field(self, key: str, value: str) -> None:
        self._vector_processor.delete_by_metadata_field(key, value)

    def search_by_vector(self, query: str, **kwargs: Any) -> list[Document]:
        query_vector = generate_embedding_of_model(
            self._knowledgebase.embedding_model, [query]
        )[0]
        return self._vector_processor.search_by_vector(query_vector, **kwargs)

    def search_by_full_text(self, query: str, **kwargs: Any) -> list[Document]:
        return self._vector_processor.search_by_full_text(query, **kwargs)

    def delete(self) -> None:
        self._vector_processor.delete()

    def init_collection(self, **kwargs):
        return self._vector_processor.init_collection(**kwargs)

    def _filter_duplicate_texts(self, texts: list[Document]) -> list[Document]:
        for text in texts:
            doc_id = text.metadata["doc_id"]
            exists_duplicate_node = self.text_exists(doc_id)
            if exists_duplicate_node:
                texts.remove(text)

        return texts

    def __getattr__(self, name):
        if self._vector_processor is not None:
            method = getattr(self._vector_processor, name)
            if callable(method):
                return method

        raise AttributeError(f"'vector_processor' object has no attribute '{name}'")
