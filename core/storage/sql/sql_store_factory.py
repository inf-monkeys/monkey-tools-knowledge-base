from core.models.sql_knowledge_base import SqlKnowledgeBaseEntity
from core.models.table import Table
from core.storage.sql.sql_store_base import BaseSQLStore
from core.config import sql_store_config

sql_store_type = sql_store_config.get("type", "sqlite")


class SqlStoreFactory:
    def __init__(self, knowledgebase: SqlKnowledgeBaseEntity):
        self._knowledgebase = knowledgebase
        self._sql_processor = self._init_sql_store()

    def _init_sql_store(self) -> BaseSQLStore:
        if not sql_store_type:
            raise ValueError("Sql store must be specified.")

        if sql_store_type == "sqlite":
            from core.storage.sql.sqlite.sqlite_store import SqliteSqlStore

            knowledge_base_id = self._knowledgebase.id
            database_name = SqlKnowledgeBaseEntity.gene_database_name_by_id(
                knowledge_base_id
            )
            return SqliteSqlStore(
                database_name=database_name,
            )
        else:
            raise ValueError(f"SQL store {sql_store_type} is not supported.")

    def create_table(self, **kwargs) -> bool:
        return self._sql_processor.create_table(**kwargs)

    def create_database(self):
        return self._sql_processor.create_database()

    def get_tables(self) -> list[Table]:
        return self._sql_processor.get_tables()

    def drop_database(self):
        self._sql_processor.drop_database()

    def import_csv(self, **kwargs):
        return self._sql_processor.import_csv(**kwargs)

    def list_table_records(self, table_name: str, **kwargs):
        return self._sql_processor.list_table_records(table_name, **kwargs)

    def drop_table(self, table_name: str):
        return self._sql_processor.drop_table(table_name)
