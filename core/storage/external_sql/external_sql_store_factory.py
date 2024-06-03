from core.models.sql_knowledge_base import SqlKnowledgeBaseEntity
from core.models.table import Table
from core.storage.external_sql.external_sql_store_base import BaseExternalSQLStore


class ExternalSqlStoreFactory:
    def __init__(self, knowledgebase: SqlKnowledgeBaseEntity):
        self._knowledgebase = knowledgebase
        self._sql_processor = self._init_sql_store()

    def _init_sql_store(self) -> BaseExternalSQLStore:
        if self._knowledgebase.database_type == "postgres":
            from core.storage.external_sql.postgres.postgres_external_store import (
                PostgresExterbalSqlStore,
            )
            from core.storage.external_sql.postgres.postgres_external_store import (
                ExternalPostgresConfig,
            )

            config = ExternalPostgresConfig(
                host=self._knowledgebase.host,
                port=self._knowledgebase.port,
                username=self._knowledgebase.username,
                password=self._knowledgebase.password,
                schema=self._knowledgebase.schema,
                database=self._knowledgebase.database,
            )
            return PostgresExterbalSqlStore(config=config)
        else:
            raise ValueError(
                f"SQL store {self._knowledgebase.database_type} is not supported."
            )

    def get_tables(self) -> list[Table]:
        return self._sql_processor.get_tables()

    def list_table_records(self, table_name: str, **kwargs):
        return self._sql_processor.list_table_records(table_name, **kwargs)

    def execute_sql(self, sql: str):
        return self._sql_processor.execute_sql(sql)
