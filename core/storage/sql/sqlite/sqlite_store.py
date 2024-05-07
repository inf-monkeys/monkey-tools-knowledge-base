from core.models.table import Table
from core.storage.sql.sql_store_base import BaseSQLStore
import sqlite3
from sqlite3 import Connection
from core.config import SQLITE_FILE_FOLDER
import os
import pandas


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

    def import_csv(self, **kwargs):
        csvfile = kwargs.get("csvfile")
        table_name = kwargs.get("table_name")
        sep = kwargs.get("sep", ",")
        if not csvfile:
            raise ValueError("csvfile must be specified.")
        df = pandas.read_csv(csvfile, sep=sep)

        # Clean and convert column names to snake case
        def clean_and_convert_column_names(columns):
            clean_columns = []
            for col in columns:
                clean_col = col.strip()
                snake_case_col = "_".join(clean_col.lower().split()).replace("-", "_")
                clean_columns.append(snake_case_col)
            return clean_columns

        df.columns = clean_and_convert_column_names(df.columns)
        df.to_sql(
            table_name, self._client, if_exists="append", index=False, chunksize=1000
        )
