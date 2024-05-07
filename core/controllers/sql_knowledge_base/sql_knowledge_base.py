from loguru import logger
from core.middleware.db import db
from flask import request, jsonify
from flask_restx import Resource
from core.models.sql_knowledge_base import SqlKnowledgeBaseEntity
import uuid
from core.storage.sql.sql_store_factory import SqlStoreFactory


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
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity(
                id=str(uuid.uuid4()),
            )
            db.session.add(sql_knowledge_base_entity)
            db.session.commit()

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

        @sql_knowledge_base_ns.doc("delete_sql_knowledge_base")
        def get(self, sql_knowledge_base_id):
            """Get tables from a sql knowledge base"""
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            tables = sql_store.get_tables()
            return {"tables": [table.serialize() for table in tables]}

        @sql_knowledge_base_ns.doc("create_sql_knowledge_base")
        def post(self, sql_knowledge_base_id):
            """Create a new table in a sql knowledge base"""
            sql = request.json.get("sql")
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            success = sql_store.create_table(sql=sql)
            return {"success": success}

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
            sql_knowledge_base_entity = SqlKnowledgeBaseEntity.get_by_id(
                sql_knowledge_base_id
            )
            sql_store = SqlStoreFactory(knowledgebase=sql_knowledge_base_entity)
            success = sql_store.import_csv(csvfile=csvfile, table_name=table_name)
            return {"success": success}
