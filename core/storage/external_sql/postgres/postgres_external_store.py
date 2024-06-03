from pydantic import BaseModel
from core.models.table import Table
from core.storage.external_sql.external_sql_store_base import BaseExternalSQLStore
import psycopg2


class ExternalPostgresConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str
    schema: str
    database: str

    def validate_config(cls, values: dict) -> dict:
        if not values["host"]:
            raise ValueError("host is required")
        if not values["port"]:
            raise ValueError("port is required")
        if not values["username"]:
            raise ValueError("username is required")
        if not values["password"]:
            raise ValueError("password is required")
        if not values["schema"]:
            raise ValueError("schema is required")
        if not values["database"]:
            raise ValueError("database is required")
        return values


class PostgresExterbalSqlStore(BaseExternalSQLStore):
    def __init__(self, config: ExternalPostgresConfig):
        self._config = config
        self._conn = self._create_connection(config)

    def _create_connection(self, config: ExternalPostgresConfig):
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            database=config.database,
        )
        return conn

    def get_type(self) -> str:
        return "postgres"

    # 函数：获取建表语句
    def get_table_create_statement(self, table_name):
        cur = self._conn.cursor()

        # 查询表的列信息
        cur.execute(
            f"""
            SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """
        )
        columns = cur.fetchall()

        # 查询表的主键信息
        cur.execute(
            f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'
        """
        )
        primary_keys = [row[0] for row in cur.fetchall()]

        # 查询列的注释信息
        cur.execute(
            f"""
            SELECT a.attname, d.description
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_description d ON a.attrelid = d.objoid AND a.attnum = d.objsubid
            WHERE c.relname = '{table_name}' AND a.attnum > 0 AND NOT a.attisdropped
        """
        )
        comments = {row[0]: row[1] for row in cur.fetchall()}

        # 构建建表语句
        create_table_statement = f"CREATE TABLE {table_name} (\n"
        column_definitions = []
        for column in columns:
            column_name, data_type, char_max_length, is_nullable, column_default = (
                column
            )
            col_def = f"  {column_name} {data_type}"
            if char_max_length:
                col_def += f"({char_max_length})"
            if column_default:
                col_def += f" DEFAULT {column_default}"
            if is_nullable == "NO":
                col_def += " NOT NULL"
            column_definitions.append(col_def)
        create_table_statement += ",\n".join(column_definitions)
        if primary_keys:
            create_table_statement += f",\n  PRIMARY KEY ({', '.join(primary_keys)})"
        create_table_statement += "\n);"

        for column_name, comment in comments.items():
            if comment:
                create_table_statement += (
                    f"\nCOMMENT ON COLUMN {table_name}.{column_name} IS '{comment}';"
                )

        return create_table_statement

    def get_tables(self) -> list[Table]:
        sql = f"""
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = '{self._config.schema}'
        """
        cur = self._conn.cursor()
        cur.execute(sql)
        raw_tables = cur.fetchall()
        data = [
            Table(
                name=table[0],
                sql=self.get_table_create_statement(table[0]),
            )
            for table in raw_tables
        ]
        self._conn.close()
        return data

    def list_table_records(self, table_name, **kwargs):
        page = kwargs.get("page", 1)
        limit = kwargs.get("limit", 10)
        sql = f"select * from {table_name} limit {limit} offset {(page - 1) * limit}"
        cur = self._conn.cursor()
        cur.execute(sql)
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            result.append(row_dict)
        self._conn.close()
        return result

    def execute_sql(self, sql):
        cur = self._conn.cursor()
        cur.execute(sql)
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = []
        for row in rows:
            row_dict = dict(zip(col_names, row))
            result.append(row_dict)
        self._conn.close()
        return result
