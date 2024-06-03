import ssl
import logging

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

from core.config import config_data
from flask import Flask, request
from flask_restx import Api
from core.config import database_config
from core.middleware import db
from core.models import load_models
from core.controllers import register_controllers
from flask_migrate import Migrate

app = Flask(__name__)
database_url = database_config.get("url", "sqlite:///data.sqlite")
pool_config = database_config.get("pool", {})
app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_size": pool_config.get("pool_size", 30),
    "pool_recycle": pool_config.get("pool_recycle", 3600),
}
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

load_models()

db.init_app(app)
migrate = Migrate(app, db.db)

api = Api(
    app,
    version="1.0",
    title="Monkeys Knowledge Base API",
    description="Monkeys Knowledge Base API",
)
register_controllers(api)


@app.before_request
def before_request():
    request.app_id = request.headers.get("x-monkeys-appid")
    request.user_id = request.headers.get("x-monkeys-userid")
    request.team_id = request.headers.get("x-monkeys-teamid")
    request.workflow_id = request.headers.get("x-monkeys-workflowid")
    request.workflow_instance_id = request.headers.get("x-monkeys-workflow-instanceid")


@api.errorhandler(Exception)
def handle_exception(error):
    return {"message": str(error)}, 500


@app.get("/manifest.json")
def get_manifest():
    return {
        "schema_version": "v1",
        "display_name": "知识库",
        "namespace": "monkey_tools_knowledge_base",
        "auth": {"type": "none"},
        "api": {"type": "openapi", "url": "/swagger.json"},
        "contact_email": "dev@inf-monkeys.com",
    }


class NoSuccessfulRequestLoggingFilter(logging.Filter):
    def filter(self, record):
        return "GET /" not in record.getMessage()


# 获取 Flask 的默认日志记录器
log = logging.getLogger("werkzeug")
# 创建并添加过滤器
log.addFilter(NoSuccessfulRequestLoggingFilter())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config_data.get("server", {}).get("port", 5000))
