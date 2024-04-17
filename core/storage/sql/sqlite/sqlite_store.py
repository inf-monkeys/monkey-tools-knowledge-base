from core.models.table import Table
from core.storage.sql.sql_store_base import BaseSQLStore
import sqlite3
from sqlite3 import Connection
from core.config import SQLITE_FILE_FOLDER
import os


class SqliteSqlStore(BaseSQLStore):
    def __init__(self, database_name: str):
        super().__init__(database_name)
        self.sqlite_file = os.path.join(SQLITE_FILE_FOLDER, f"{database_name}.db")
        self._client = self._init_client()
        self._consistency_level = "Session"
        self._fields = []

    def get_type(self) -> str:
        return "sqlite"

    def _init_client(self) -> Connection:
        return sqlite3.connect(self.sqlite_file)

    def create_table(self, **kwargs) -> bool:
        sql = kwargs.get("sql")
        cur = self._client.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        success = result != None
        return success

    def create_database(self, **kwargs):
        pass

    def get_tables(self) -> list[Table]:
        sql = """
SELECT *
FROM sqlite_master
WHERE type='table';
        """
        cur = self._client.cursor()
        cur.execute(sql)
        raw_tables = cur.fetchall()

        return [
            Table(
                name=table[1],
                sql=table[4],
            )
            for table in raw_tables
        ]

    def drop_database(self, **kwargs):
        os.remove(self.sqlite_file)
