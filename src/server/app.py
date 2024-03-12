from flask import Flask, request, jsonify
from flask_restx import Api
from src.config import database_url
import traceback

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = database_url
api = Api(app, version='1.0', title='TodoMVC API',
          description='A simple TodoMVC API',
          )


@app.before_request
def before_request():
    request.app_id = request.headers.get('x-monkeys-appid')
    request.user_id = request.headers.get('x-monkeys-userid')
    request.team_id = request.headers.get('x-monkeys-teamid')
    request.workflow_instance_id = request.headers.get('x-monkeys-workflow-instanceid')


@app.errorhandler(Exception)
def handle_exception(error):
    traceback.print_exc()
    response = {'message': str(error)}
    response['code'] = 500

    return jsonify(response), response['code']


@app.get("/manifest.json")
def get_manifest():
    return {
        "schema_version": "v1",
        "namespace": 'monkeys_tools_vector',
        "auth": {
            "type": "none"
        },
        "api": {
            "type": "openapi",
            "url": "/swagger.json"
        },
        "contact_email": "dev@inf-monkeys.com",
    }
