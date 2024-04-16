from . import documents
from . import knowledge_base
from . import segments
from . import search
from . import tasks


def regieter(api):
    documents.register(api)
    knowledge_base.register(api)
    segments.register(api)
    search.register(api)
    tasks.register(api)

