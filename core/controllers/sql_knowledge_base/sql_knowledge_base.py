from loguru import logger
from core.middleware.db import db
from flask import request, jsonify
from flask_restx import Resource
from core.models.sql_knowledge_base import SqlKnowledgeBaseEntity
import uuid
from core.storage.sql.sql_store_factory import SqlStoreFactory
from core.storage.external_sql.external_sql_store_factory import ExternalSqlStoreFactory


def register(api):
    sql_knowledge_base_ns = api.namespace(
        "sql-knowledge-bases", description="SQL Knowledge Bases operations"
    )

    @sql_knowledge_base_ns.route("/")
    class SqlKnowledgeBaseList(Resource):
        """Create Sql Knowledge Base"""

        @sql_knowledge_base_ns.doc("create_sql_knowledge_base")
        def post(self):
            """Create a new sql knowledge base"""

            data = request.json
            type = data.get("createType", "builtIn")

            external_database_type = None
            host = None
            port = None
            username = None
            password = None
            schema = None
            database = None

            if type == "external":
                external_database_type = data.get("externalDatabaseType")
                external_database_connection_options = data.get(
                    "externalDatabaseConnectionOptions"
                )
                host = external_database_connection_options.get("host")
                port = external_database_connection_options.get("port")
                username = external_database_connection_options.get("username")
                password = external_database_connection_options.get("password")
                schema = external_database_connection_options.get("schema", "public")
                database = external_database_connection_options.get("database")

                if (
                    not host
                    or not port
                    or not username
                    or not password
                    or not schema
                    or not database
                ):
                    return {"error": "Missing required fields"}, 400

                port = int(port)

            sql_knowledge_base_entity = SqlKnowledgeBaseEntity(
                id=str(uuid.uuid4()),
                type=type,
                database_type=external_database_type,
                host=host,
                port=port,
                username=username,
                password=password,
                schema=schema,
                database=database,
            )
            db.session.add(sql_knowledge_base_entity)
            db.session.commit()

            if type == "builtIn":
                sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
                sql_store.create_database()

            return jsonify(sql_knowledge_base_entity.serialize())

    @sql_knowledge_base_ns.route("/<string:sql_knowledge_base_id>")
    class SqlKnowledgeBaseDetail(Resource):
        """Manage Sql Knowledge Base Detail"""

        @sql_knowledge_base_ns.doc("delete_sql_knowledge_base")
        def delete(self, sql_knowledge_base_id):
            """Delete sql knowledge base"""
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            sql_store.drop_database()
            SqlKnowledgeBaseEntity.delete_by_id(sql_knowledge_base_id)

            return jsonify(sql_knowledge_base_entity.serialize())

    @sql_knowledge_base_ns.route("/<string:sql_knowledge_base_id>/tables")
    @sql_knowledge_base_ns.response(404, "Sql Knowledge base not found")
    @sql_knowledge_base_ns.param(
        "sql_knowledge_base_id", "The sql knowledge base identifier"
    )
    class SqlKnowledgeBaseTables(Resource):
        """Manage Sql Knowledge Base Tables"""

        @sql_knowledge_base_ns.doc("list_sql_knowledge_base_tables")
        def get(self, sql_knowledge_base_id):
            """Get tables from a sql knowledge base"""
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )

            tables = []
            if sql_knowledge_base_entity.type == "external":
                external_sql_store = ExternalSqlStoreFactory(
                    knowledgebase=sql_knowledge_base_entity
                )
                tables = external_sql_store.get_tables()
                tables = [table.serialize() for table in tables]
            else:
                sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
                tables = sql_store.get_tables()
                tables = [table.serialize() for table in tables]
            return {"tables": tables}

        @sql_knowledge_base_ns.doc("create_sql_knowledge_base_table")
        def post(self, sql_knowledge_base_id):
            """Create a new table in a sql knowledge base"""
            sql = request.json.get("sql")
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            success = sql_store.create_table(sql=sql)
            return {"success": success}

    @sql_knowledge_base_ns.route(
        "/<string:sql_knowledge_base_id>/tables/<string:table_name>"
    )
    @sql_knowledge_base_ns.response(404, "Sql Knowledge base not found")
    @sql_knowledge_base_ns.param(
        "sql_knowledge_base_id", "The sql knowledge base identifier"
    )
    @sql_knowledge_base_ns.param("table_id", "The table name")
    class SqlKnowledgeBaseTables(Resource):
        """Manage Sql Knowledge Base Table Records"""

        @sql_knowledge_base_ns.doc("query_table")
        @sql_knowledge_base_ns.vendor(
            {
                "x-monkey-tool-name": "query_table",
                "x-monkey-tool-categories": ["query"],
                "x-monkey-tool-display-name": "查询表格数据",
                "x-monkey-tool-description": "查询表格数据",
                "x-monkey-tool-icon": "emoji:📊:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": "文本数据库",
                        "name": "sql_knowledge_base_id",
                        "type": "string",
                        "typeOptions": {"assetType": "sql-knowledge-base"},
                        "default": "",
                        "required": True,
                    },
                    {
                        "diaplasyName": "查询模式",
                        "name": "queryMode",
                        "type": "options",
                        "default": "simple",
                        "options": [
                            {
                                "name": "simple",
                                "value": "simple",
                            },
                            {"name": "sql", "value": "sql"},
                        ],
                    },
                    {
                        "displayName": "表名",
                        "name": "table_name",
                        "type": "string",
                        "default": "",
                        "required": False,
                        "displayOptions": {
                            "show": {
                                "queryMode": ["simple"],
                            }
                        },
                    },
                    {
                        "displayName": "Page",
                        "name": "page",
                        "type": "number",
                        "default": 1,
                        "required": False,
                        "displayOptions": {
                            "show": {
                                "queryMode": ["simple"],
                            }
                        },
                    },
                    {
                        "displayName": "Limit",
                        "name": "limit",
                        "type": "number",
                        "default": 10,
                        "required": False,
                        "displayOptions": {
                            "show": {
                                "queryMode": ["simple"],
                            }
                        },
                    },
                    {
                        "displayName": "SQL 查询语句",
                        "name": "sql",
                        "type": "string",
                        "default": "",
                        "required": False,
                        "displayOptions": {
                            "show": {
                                "queryMode": ["sql"],
                            }
                        },
                    },
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 5,
                },
            }
        )
        def get(self, sql_knowledge_base_id, table_name):
            """List records from a table"""
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )

            json = request.json
            query_mode = json.get("queryMode", "simple")

            records = []
            if query_mode == "simple":
                page = json.get("page", 1)
                page = int(page)
                limit = json.get("limit", 10)
                limit = int(limit)
                if sql_knowledge_base_entity.type == "external":
                    external_sql_store = ExternalSqlStoreFactory(
                        knowledgebase=sql_knowledge_base_entity
                    )
                    records = external_sql_store.list_table_records(
                        table_name=table_name, page=page, limit=limit
                    )
                else:
                    sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
                    records = sql_store.list_table_records(
                        table_name=table_name, page=page, limit=limit
                    )
            elif query_mode == "sql":
                sql = json.get("sql")
                if not sql:
                    return {"error": "Missing required fields"}, 400
                if sql_knowledge_base_entity.type == "external":
                    external_sql_store = ExternalSqlStoreFactory(
                        knowledgebase=sql_knowledge_base_entity
                    )
                    records = external_sql_store.execute_sql(sql)
                else:
                    sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
                    records = sql_store.execute_sql(sql)

            return jsonify({"records": records})

        @sql_knowledge_base_ns.doc("delete_table")
        def delete(self, sql_knowledge_base_id, table_name):
            """Delete table"""
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            success = sql_store.drop_table(table_name)
            return {"success": success}

    @sql_knowledge_base_ns.route("/<string:sql_knowledge_base_id>/sql")
    @sql_knowledge_base_ns.response(404, "Sql Knowledge base not found")
    @sql_knowledge_base_ns.param(
        "sql_knowledge_base_id", "The sql knowledge base identifier"
    )
    class SqlKnowledgeBaseExecuteSql(Resource):
        """Manage Sql Knowledge Base Table Records"""

        @sql_knowledge_base_ns.doc("query_table_sql")
        @sql_knowledge_base_ns.vendor(
            {
                "x-monkey-tool-name": "query_table_sql",
                "x-monkey-tool-categories": ["query"],
                "x-monkey-tool-display-name": "使用 SQL 查询表格数据",
                "x-monkey-tool-description": "使用 SQL 查询表格数据",
                "x-monkey-tool-icon": "emoji:📊:#e58c3a",
                "x-monkey-tool-input": [
                    {
                        "displayName": "文本数据库",
                        "name": "sql_knowledge_base_id",
                        "type": "string",
                        "typeOptions": {"assetType": "sql-knowledge-base"},
                        "default": "",
                        "required": True,
                    },
                    {
                        "displayName": "SQL 查询语句",
                        "name": "sql",
                        "type": "string",
                        "default": "",
                        "required": True,
                    },
                ],
                "x-monkey-tool-extra": {
                    "estimateTime": 5,
                },
            }
        )
        def post(self, sql_knowledge_base_id):
            """List records from a table"""
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            json = request.json
            records = []
            sql = json.get("sql")
            if not sql:
                return {"error": "Missing required fields"}, 400
            if sql_knowledge_base_entity.type == "external":
                external_sql_store = ExternalSqlStoreFactory(
                    knowledgebase=sql_knowledge_base_entity
                )
                records = external_sql_store.execute_sql(sql)
            else:
                sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
                records = sql_store.execute_sql(sql)

            return jsonify({"records": records})

    @sql_knowledge_base_ns.route("/<string:sql_knowledge_base_id>/csvs")
    @sql_knowledge_base_ns.response(404, "Sql Knowledge base not found")
    @sql_knowledge_base_ns.param(
        "sql_knowledge_base_id", "The sql knowledge base identifier"
    )
    class SqlKnowledgeBaseCSVs(Resource):
        """Manage Sql Knowledge Base CSVs"""

        @sql_knowledge_base_ns.doc("import_csv")
        def post(self, sql_knowledge_base_id):
            """Import csv to a sql knowledge base"""
            csvfile = request.json.get("csvfile")
            table_name = request.json.get("table_name")
            sep = request.json.get("sep", ",")
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            success = sql_store.import_csv(
                csvfile=csvfile, table_name=table_name, sep=sep
            )
            return {"success": success}
