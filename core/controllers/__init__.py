from . import helpers
from . import knowledge_base
from . import sql_knowledge_base

def register_controllers(api):
    helpers.register(api)
    knowledge_base.register(api)
    sql_knowledge_base.register(api)
