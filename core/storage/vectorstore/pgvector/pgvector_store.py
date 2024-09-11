from pydantic import BaseModel
from core.models.document import Document
from core.storage.vectorstore.vector_store_base import BaseVectorStore
from sqlalchemy import create_engine, Column, String, Text, JSON, select, DateTime, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from core.utils import chunk_list, generate_md5
from pgvector.sqlalchemy import Vector
from sqlalchemy.sql.expression import text
from sqlalchemy.dialects.postgresql import array

Base = declarative_base()


class PGVectorConfig(BaseModel):
    url: str
    pool_size: int = 5
    max_overflow: int = 10
    batch_size: int = 100

    def validate_config(cls, values: dict) -> dict:
        if not values["url"]:
            raise ValueError("config vector.pgvector.url is required")
        return values


session = None
engine = None


def create_session(config: PGVectorConfig):
    global engine
    global session
    
    if session and engine:
        return engine, session
    
    engine = create_engine(config.url, pool_size=config.pool_size, max_overflow=config.max_overflow)
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session


class PGVectorStore(BaseVectorStore):
    def __init__(self, collection_name: str, dimension: int, config: PGVectorConfig):
        super().__init__(collection_name)
        self._client_config = config
        self._engine, self._session = create_session(config)

        class PGVectorDocument(Base):
            __tablename__ = self._collection_name
            __table_args__ = {"extend_existing": True}
            id = Column(String(64), primary_key=True)
            meta_data = Column(JSON)
            page_content = Column(Text)
            created_at = Column(
                DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP(0)")
            )
            updated_at = Column(
                DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP(0)")
            )
            embeddings = Column(Vector(dimension))  # 使用 BYTEA 存储向量数据

        self._table = PGVectorDocument

    def create_collection(self, **kwargs) -> BaseVectorStore:
        dimension = kwargs.get("dimension")
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    f"""
                    CREATE TABLE {self._collection_name} (
                        id varchar(64) PRIMARY KEY,
                        meta_data jsonb,
                        page_content text,
                        embeddings vector({dimension}),
                        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP(0)
                    )
                    """
                )
                # Add full text index on page_content
                conn.execute(
                    f"""
                    CREATE INDEX idx_{self._collection_name}_page_content
                    ON {self._collection_name}
                    USING gin(to_tsvector('english', page_content))
                    """
                )
            return self
        except Exception as e:
            # 如果发生错误，事务会自动回滚
            raise e

    def add_texts(
        self,
        texts: list[Document],
        embeddings: list[list[float]],
    ):
        db_documents = [
            self._table(
                id=generate_md5(doc.page_content),
                page_content=doc.page_content,
                meta_data=doc.metadata,
                embeddings=embeddings[index],
            )
            for index, doc in enumerate(texts)
        ]
        chunks = chunk_list(db_documents, self._client_config.batch_size)
        for chunk in chunks:
            data_ids = [d.id for d in chunk]
            existing_records = self._session.query(self._table).filter(
                self._table.id.in_(data_ids)
            ).all()
            existing_ids = {record.id for record in existing_records}

            updates = []
            creates = []
            for d in chunk:
                if d.id in existing_ids:
                    updates.append(d)
                else:
                    creates.append(d)

            for update in updates:
                existing_record = next(r for r in existing_records if r.id == update.id)
                existing_record.page_content = update.page_content
                existing_record.meta_data = update.meta_data
                existing_record.embeddings = update.embeddings

            self._session.bulk_save_objects(creates)

        try:
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e

    def delete_by_ids(self, ids: list[str]) -> None:
        try:
            self._session.query(self._table).filter(self._table.id.in_(ids)).delete(
                synchronize_session=False
            )
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e

    def delete_by_metadata_field(self, key: str, value: str) -> None:
        try:
            q = self._session.query(self._table)
            q = q.filter(text(f"meta_data->>'{key}' = :value")).params(value=value)
            q.delete(synchronize_session=False)
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            raise e

    def search_by_full_text(self, query: str, **kwargs) -> list[Document]:
        metadata_filter = kwargs.get("metadata_filter", None)
        from_ = kwargs.get("from_", 0)
        size = kwargs.get("size", 10)
        sort_by_created_at = kwargs.get("sort_by_created_at", False)

        q = self._session.query(self._table)
        if query:
            q = q.filter(
                text(
                    "to_tsvector('english', page_content) @@ plainto_tsquery('english', :query)"
                )
            ).params(query=query)
        # 如果提供了元数据过滤条件
        if metadata_filter:
            for key, value in metadata_filter.items():
                q = q.filter(text(f"meta_data->>'{key}' = :value")).params(value=value)
        if sort_by_created_at:
            q = q.order_by(self._table.created_at.desc())
        q = q.offset(from_).limit(size)

        results = q.all()
        return [
            Document(
                pk=result.id,
                page_content=result.page_content,
                metadata=result.meta_data,
            )
            for result in results
        ]

    def search_by_vector(self, query_vector: list[float], **kwargs) -> list[Document]:
        top_k = kwargs.get("top_k", 3)
        metadata_filter = kwargs.get("metadata_filter", None)

        query = select(self._table)
        if metadata_filter:
            filters = []
            for key, value in metadata_filter.items():
                if not value:
                    continue
                if isinstance(value, list):
                    filters.append(text(f"meta_data->>'{key}' = ANY(:{key})").params({key: value}))
                elif isinstance(value, str):
                    filters.append(text(f"meta_data->>'{key}' = :{key}").params({key: value}))
                elif isinstance(value, int):
                    filters.append(text(f"meta_data->>'{key}' = :{key}").params({key: str(value)}))
    
            if filters:
                query = query.filter(and_(*filters))
    
        results = self._session.scalars(
            query
            .order_by(self._table.embeddings.l2_distance(query_vector))
            .limit(top_k)
        )
        return [
            Document(
                pk=result.id,
                page_content=result.page_content,
                metadata=result.meta_data,
            )
            for result in results
        ]

    def text_exists(self, id: str) -> bool:
        return super().text_exists(id)

    def update_by_id(self, id: str, document: Document) -> None:
        try:
            record = self._session.query(self._table).get(id)
            if record:
                record.page_content = document.page_content
                record.meta_data = document.metadata
                self._session.commit()
            else:
                raise ValueError(f"No record found with id: {id}")
        except Exception as e:
            self._session.rollback()
            raise e

    def delete(self) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {self._collection_name}")
        except Exception as e:
            # 如果发生错误，事务会自动回滚
            raise e

    def get_metadata_key_unique_values(self, key: str) -> list[str]:
        try:
            sql = f"""SELECT DISTINCT meta_data->>'{key}' AS filename FROM "public"."{self._collection_name}";"""
            with self._engine.connect() as conn:
                result = conn.execute(sql)
                return [row[0] for row in result if row[0] is not None]
        except Exception as e:
            # 记录错误或进行其他错误处理
            raise e
