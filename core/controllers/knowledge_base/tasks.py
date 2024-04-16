from flask import request, jsonify
from flask_restx import Resource
from core.models.knowledge_base import KnowledgeBaseEntity
from core.models.task import TaskEntity


def register(api):
    knowledge_base_ns = api.namespace(
        "knowledge-bases", description="Knowledge Bases operations"
    )

    @knowledge_base_ns.route("/<string:knowledge_base_id>/tasks")
    @knowledge_base_ns.param("knowledge_base_id", "The knowledge base identifier")
    class KnowledgeBaseTasks(Resource):
        """Shows a list of all todos, and lets you POST to add new tasks"""

        @knowledge_base_ns.doc("list_tasks")
        def get(self, knowledge_base_id):
            """List all Tasks"""
            KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            tasks = TaskEntity.find_by_knowledge_base_id(knowledge_base_id)
            return jsonify({"list": [task.serialize() for task in tasks]})

    @knowledge_base_ns.route("/<string:knowledge_base_id>/tasks/<string:task_id>")
    @knowledge_base_ns.response(404, "Knowledge base not found")
    @knowledge_base_ns.param("knowledge_base_name", "The knowledge base identifier")
    @knowledge_base_ns.param("task_id", "The Task identifier")
    class TaskDetail(Resource):
        """Shows a list of all todos, and lets you POST to add new tasks"""

        @knowledge_base_ns.doc("get_task_detail")
        def get(self, knowledge_base_id, task_id):
            """Get A Task Detail"""
            KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            task = TaskEntity.get_by_id(task_id)
            return jsonify(task.serialize() if task else {})
