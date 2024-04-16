from . import helpers
from . import knowledge_base


def register_controllers(api):
    helpers.register(api)
    knowledge_base.regieter(api)
