import uuid

from .app import api, app
from flask import request
from flask_restx import Api, Resource, fields
from src.database import CollectionTable, FileRecord, FileProcessProgressTable
from bson.json_util import dumps
import json

from ..es import ESClient
from ..queue import submit_task, PROCESS_FILE_QUEUE_NAME
from ..utils import generate_random_string, get_dimension_by_embedding_model, generate_short_id, \
    generate_embedding_of_model, generate_md5

collection_ns = api.namespace('collections', description='Collection operations')
collection_model = api.model('Collection', {
    'id': fields.String(readonly=True, description='The collection unique identifier'),
    'createdTimestamp': fields.Integer(readonly=True, description='Create Timestamp'),
    'updatedTimestamp': fields.Integer(readonly=True, description='Update Timestamp'),
    'isDeleted': fields.Boolean(readonly=True, description='Is Deleted'),
    'creatorUserId': fields.String(readonly=True, description='Creator User Id'),
    'teamId': fields.String(readonly=True, description='Team Id'),
    'name': fields.String(readonly=True, description='Collection name, which is unique'),
    'displayName': fields.String(description='Collection display name'),
    'description': fields.String(description='Collection description'),
})


@collection_ns.route('/')
class CollectionList(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-hidden": True,
    })
    @collection_ns.doc('list_collections')
    @collection_ns.marshal_list_with(collection_model)
    def get(self):
        '''List all Collections'''
        team_id = request.team_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        data = table.find_by_team(team_id=team_id)
        data = json.loads(dumps(data))
        file_record_table = FileRecord(app_id=app_id)
        for item in data:
            es_client = ESClient(app_id=app_id, index_name=item['name'])
            entity_count = es_client.count_documents()
            item['entityCount'] = entity_count
            file_count = file_record_table.get_file_count(team_id, item['name'])
            item['fileCount'] = file_count
        return data

    @collection_ns.doc('create_collection')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    def post(self):
        '''Create a new Collection'''
        app_id = request.app_id
        user_id = request.user_id
        team_id = request.team_id

        data = request.json
        displayName = data.get('displayName')
        logo = data.get('logo')
        name = generate_random_string()
        embedding_model = data.get('embeddingModel')
        metadata_fields = data.get('metadataFields', None)
        description = data.get('description', '')
        dimension = get_dimension_by_embedding_model(embedding_model)
        table = CollectionTable(
            app_id=app_id
        )
        conflict = table.check_name_conflicts(name)
        if conflict:
            raise Exception(f"唯一标志 {name} 已被占用，请换一个")

        # 在 es 中创建 template
        es_client = ESClient(
            app_id=app_id,
            index_name=name
        )
        es_client.create_es_index(dimension)
        table.insert_one(
            creator_user_id=user_id,
            team_id=team_id,
            name=name,
            display_name=displayName,
            description=description,
            embedding_model=embedding_model,
            dimension=dimension,
            logo=logo,
            metadata_fields=metadata_fields
        )

        return {
            "success": True,
            "name": name
        }


@collection_ns.route('/<string:name>')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionDetail(Resource):
    '''Show a single todo item and lets you delete them'''

    @collection_ns.doc('get_collection')
    @collection_ns.marshal_with(collection_model)
    def get(self, name):
        '''Fetch a given collection'''
        team_id = request.team_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        data = table.find_by_name(team_id, name)
        return dumps(data)

    @collection_ns.doc('delete_collection')
    @collection_ns.response(204, 'Collection deleted')
    def delete(self, name):
        '''Delete a collection given its identifier'''
        team_id = request.team_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        table.delete_by_name(team_id, name)
        es_client = ESClient(app_id=app_id, index_name=name)
        es_client.delete_index()
        return {
            "success": True
        }

    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model)
    def put(self, name):
        '''Update a collection given its identifier'''
        team_id = request.team_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name(team_id, name)
        if not collection:
            raise Exception("数据集不存在")

        data = request.json
        description = data.get('description')
        display_name = data.get('displayName')
        logo = data.get('logo')

        table.update_by_name(
            team_id,
            name,
            description=description,
            display_name=display_name,
            logo=logo,
        )
        return {
            "success": True
        }


@collection_ns.route('/<string:name>/data')
@collection_ns.param('name', 'The collection identifier')
class CollectionData(Resource):
    '''Show a single todo item and lets you delete them'''

    @collection_ns.doc('delete_collection_data')
    def delete(self, name):
        '''Delete data in a collection'''
        app_id = request.app_id
        es_client = ESClient(app_id=app_id, index_name=name)
        # 删除索引
        es_client.delete_index()
        # 重新创建个新的
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name_without_team(name)
        es_client.create_es_index(
            dimension=collection['dimension']
        )
        return {
            "success": True
        }


@collection_ns.route('/<string:name>/authorization')
@collection_ns.param('name', 'The collection identifier')
class CollectionAuthorization(Resource):
    '''Show a single todo item and lets you delete them'''

    @collection_ns.doc('authorize_collection')
    def put(self, name):
        '''Authorize collection to other team or user'''
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name_without_team(name)
        if not collection:
            raise Exception("数据集不存在")

        data = request.json
        team_id = data.get('team_id')
        table.authorize_target(
            name,
            team_id,
        )
        return {
            "success": True
        }


@collection_ns.route('/<string:name>/copy')
@collection_ns.param('name', 'The collection identifier')
class CollectionCopy(Resource):
    '''Show a single todo item and lets you delete them'''

    @collection_ns.doc('copy_collection')
    def post(self, name):
        """Copy a collection"""
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name_without_team(name)
        if not collection:
            raise Exception("数据集不存在")

        data = request.json
        team_id = data.get('team_id')
        user_id = data.get('user_id')

        embedding_model = collection.get('embeddingModel')
        dimension = collection.get('dimension')
        new_collection_name = generate_short_id()
        description = collection.get('description')

        # 在 es 中创建 template
        es_client = ESClient(app_id=app_id, index_name=name)
        es_client.create_es_index(
            dimension
        )
        table.insert_one(
            creator_user_id=user_id,
            team_id=team_id,
            name=new_collection_name,
            display_name=collection.get('displayName'),
            description=description,
            logo=collection.get('logo'),
            embedding_model=embedding_model,
            dimension=dimension,
            metadata_fields=collection.get('metadataFields')
        )
        return {
            "name": new_collection_name
        }


@collection_ns.route('/<string:name>/tasks')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class TaskList(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-hidden": True,
    })
    @collection_ns.doc('list_tasks')
    @collection_ns.marshal_list_with(collection_model)
    def get(self, name):
        '''List all Tasks'''
        team_id = request.team_id
        app_id = request.app_id
        table = FileProcessProgressTable(
            app_id=app_id
        )
        data = table.list_tasks(
            team_id=team_id,
            collection_name=name
        )
        return dumps(data)

    @api.vendor({
        "x-monkey-tool-hidden": True,
    })
    @collection_ns.doc('create_task')
    @collection_ns.marshal_list_with(collection_model)
    def post(self, name):
        '''Create A Task'''
        team_id = request.team_id
        user_id = request.user_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name(team_id, name)
        embedding_model = collection["embeddingModel"]

        data = request.json
        file_url = data.get("fileURL")
        metadata = data.get("metadata", {})
        oss_config = data.get('ossConfig', None)
        metadata["userId"] = user_id
        split = data.get('split', {})
        params = split.get('params', {})

        # json 文件
        jqSchema = params.get('jqSchema', None)

        # 非 json 文件
        pre_process_rules = params.get('preProcessRules', [])
        segmentParams = params.get('segmentParams', {})
        chunk_overlap = segmentParams.get('segmentChunkOverlap', 10)
        chunk_size = segmentParams.get('segmentMaxLength', 1000)
        separator = segmentParams.get('segmentSymbol', "\n\n")
        task_id = str(uuid.uuid4())

        progress_table = FileProcessProgressTable(app_id)
        progress_table.create_task(
            team_id=team_id, collection_name=name, task_id=task_id
        )
        submit_task(PROCESS_FILE_QUEUE_NAME, {
            'app_id': app_id,
            'team_id': team_id,
            'user_id': user_id,
            'collection_name': name,
            'embedding_model': embedding_model,
            'file_url': file_url,
            'oss_config': oss_config,
            'metadata': metadata,
            'task_id': task_id,
            'chunk_size': chunk_size,
            'chunk_overlap': chunk_overlap,
            'separator': separator,
            'pre_process_rules': pre_process_rules,
            'jqSchema': jqSchema
        })
        return {"taskId": task_id}


@collection_ns.route('/<string:name>/tasks/<string:task_id>')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
@collection_ns.param('task_id', 'The Task identifier')
class TaskDetail(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @collection_ns.doc('get_task_detail')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    def get(self, name, task_id):
        '''Get A Task Detail'''
        team_id = request.team_id
        app_id = request.app_id
        table = FileProcessProgressTable(
            app_id=app_id
        )
        data = table.get_task(
            team_id=team_id,
            collection_name=name,
            task_id=task_id
        )
        return dumps(data)


@collection_ns.route('/<string:name>/vectors')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectors(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @collection_ns.doc('create_vector')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    def post(self, name):
        '''Create A Vector'''
        team_id = request.team_id
        user_id = request.user_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name(team_id, name)
        embedding_model = collection["embeddingModel"]
        data = request.json
        text = data.get("text")
        metadata = data.get("metadata", {})
        metadata["userId"] = user_id
        es_client = ESClient(app_id=app_id, index_name=name)
        delimiter = data.get('delimiter')
        if delimiter:
            delimiter = delimiter.replace('\\n', '\n')
            text_list = text.split(delimiter)
            text_list = [
                {
                    "page_content": item,
                    "metadata": metadata
                } for item in text_list
            ]
            es_client.insert_texts_batch(embedding_model, text_list)
            return {
                'inserted': len(text_list)
            }
        else:
            embedding = generate_embedding_of_model(embedding_model, [text])
            pk = generate_md5(text)
            es_client.upsert_document(pk, {
                "page_content": text,
                "metadata": metadata,
                "embeddings": embedding[0]
            })
            table.add_metadata_fields_if_not_exists(
                team_id, name, metadata.keys()
            )
            return {
                "pk": pk
            }


@collection_ns.route('/<string:name>/vectors/<string:pk>')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectorDetail(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @collection_ns.doc('delete_vector')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    def delete(self, name, pk):
        '''Create A Vector'''
        app_id = request.app_id
        es_client = ESClient(app_id=app_id, index_name=name)
        result = es_client.delete_es_document(pk)
        return {"result": result.body}

    @collection_ns.doc('upsert_vector')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    def put(self, name, pk):
        '''Create A Vector'''
        data = request.json
        team_id = request.team_id
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        text = data.get("text")
        metadata = data.get("metadata")
        collection = table.find_by_name(team_id, name)
        embedding_model = collection["embeddingModel"]
        embedding = generate_embedding_of_model(embedding_model, [text])
        es_client = ESClient(
            app_id=app_id,
            index_name=name
        )
        result = es_client.upsert_document(
            pk=pk,
            document={
                "page_content": text,
                "metadata": metadata,
                "embeddings": embedding[0]
            }
        )
        return {"result": result.body}


@collection_ns.route('/<string:name>/vectors-batch')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectorBatch(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @collection_ns.doc('upsert_vectors')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    def put(self, name):
        '''Create A Vector'''
        app_id = request.app_id
        table = CollectionTable(
            app_id=app_id
        )
        collection = table.find_by_name_without_team(name)
        if not collection:
            raise Exception(f"向量数据库 {name} 不存在")
        embedding_model = collection.get("embeddingModel")
        es_client = ESClient(app_id=app_id, index_name=name)
        list = request.json
        texts = [item["text"] for item in list]
        embeddings = generate_embedding_of_model(embedding_model, texts)
        es_documents = []
        for (index, item) in enumerate(list):
            es_documents.append({
                "_id": item['pk'],
                "_source": {
                    "page_content": item['text'],
                    "metadata": item['metadata'],
                    "embeddings": embeddings[index]
                }
            })
        es_client.upsert_documents_batch(
            es_documents
        )
        return {"success": True}


@collection_ns.route('/<string:name>/fulltext-search')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionFullTextSearch(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @collection_ns.doc('fulltext_search')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    @collection_ns.vendor({
        "x-monkey-tool-name": "fulltext_search_documents",
        "x-monkey-tool-categories": ['query'],
        "x-monkey-tool-display-name": '文本全文搜索',
        "x-monkey-tool-description": '对文本进行全文关键字搜索，返回最匹配的文档列表',
        "x-monkey-tool-icon": 'emoji:💿:#e58c3a',
        "x-monkey-tool-input": [
            {
                "displayName": '文本数据库',
                "name": 'collection',
                "type": 'string',
                "typeOptions": {
                    "assetType": 'text-collection'
                },
                "default": '',
                "required": True
            },
            {
                "displayName": '关键词',
                "name": 'query',
                "type": 'string',
                "default": '',
                "required": False,
            },
            {
                "displayName": 'TopK',
                "name": 'topK',
                "type": 'number',
                "default": 3,
                "required": False,
            },
            {
                "displayName": '数据过滤方式',
                "name": 'filterType',
                "type": 'options',
                "options": [
                    {
                        "name": "简单形式",
                        "value": "simple"
                    },
                    {
                        "name": "ES 表达式",
                        "value": "es-expression"
                    }
                ],
                "default": 'simple',
                "required": False,
            },
            {
                "displayName": '根据元数据的字段进行过滤',
                "name": 'metadata_filter',
                "type": 'json',
                "typeOptions": {
                    "multiFieldObject": True,
                    "multipleValues": False
                },
                "default": '',
                "required": False,
                "description": "根据元数据的字段进行过滤",
                "displayOptions": {
                    "show": {
                        "filterType": [
                            "simple"
                        ]
                    }
                }
            },
            {
                "name": "docs",
                "type": "notice",
                "displayName": """使用 ES 搜索过滤表达式用于对文本进行精准过滤。\n示例：
    ```json
    {
        "term": {
            "metadata.filename.keyword": "文件名称"
        }
    }
    ```
                """,
                "displayOptions": {
                    "show": {
                        "filterType": [
                            "es-expression"
                        ]
                    }
                }
            },
            {
                "displayName": '过滤表达式',
                "name": 'expr',
                "type": 'json',
                "required": False,
                "displayOptions": {
                    "show": {
                        "filterType": [
                            "es-expression"
                        ]
                    }
                }
            },
            {
                "displayName": '是否按照创建时间进行排序',
                "name": 'orderByCreatedAt',
                "type": 'boolean',
                "required": False,
                "default": False
            },
        ],
        "x-monkey-tool-output": [
            {
                "name": 'result',
                "displayName": '相似性集合',
                "type": 'json',
                "typeOptions": {
                    "multipleValues": True,
                },
                "properties": [
                    {
                        "name": 'metadata',
                        "displayName": '元数据',
                        "type": 'json',
                    },
                    {
                        "name": 'page_content',
                        "displayName": '文本内容',
                        "type": 'string',
                    },
                ],
            },
            {
                "name": "text",
                "displayName": "所有搜索的结果组合的字符串",
                "type": "string"
            }
        ],
        "x-monkey-tool-extra": {
            "estimateTime": 5,
        },
    })
    def post(self, name):
        '''Full Text Search'''
        app_id = request.app_id
        data = request.json
        query = data.get("query", None)
        es_client = ESClient(app_id=app_id, index_name=name)
        from_ = data.get("from", 0)
        size = data.get("size", 30)
        metadata_filter = data.get('metadataFilter', None)
        sort_by_created_at = data.get('sortByCreatedAt', False)
        hits = es_client.full_text_search(
            query=query,
            from_=from_,
            size=size,
            metadata_filter=metadata_filter,
            sort_by_created_at=sort_by_created_at
        )
        return {"hits": hits}


@collection_ns.route('/<string:name>/vector-search')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectorSearch(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @collection_ns.doc('vector_search')
    @collection_ns.expect(collection_model)
    @collection_ns.marshal_with(collection_model, code=201)
    @collection_ns.vendor({
        "x-monkey-tool-name": "search_vector",
        "x-monkey-tool-categories": ['query'],
        "x-monkey-tool-display-name": '文本向量搜索',
        "x-monkey-tool-description": '根据提供的文本对进行相似性搜索',
        "x-monkey-tool-icon": 'emoji:💿:#e58c3a',
        "x-monkey-tool-input": [
            {
                "displayName": '文本数据库',
                "name": 'collection',
                "type": 'string',
                "typeOptions": {
                    "assetType": 'text-collection'
                },
                "default": '',
                "required": True,
            },
            {
                "displayName": '关键词',
                "name": 'question',
                "type": 'string',
                "default": '',
                "required": True,
            },
            {
                "displayName": 'TopK',
                "name": 'topK',
                "type": 'number',
                "default": 3,
                "required": False,
            },
            {
                "displayName": '根据元数据字段进行过滤',
                "name": 'metadata_filter',
                "type": 'json',
                "typeOptions": {
                    "multiFieldObject": True,
                    "multipleValues": False
                },
                "default": '',
                "required": False,
                "description": "根据元数据的字段进行过滤"
            },
        ],
        "x-monkey-tool-output": [
            {
                "name": 'result',
                "displayName": '相似性集合',
                "type": 'json',
                "typeOptions": {
                    "multipleValues": True,
                },
                "properties": [
                    {
                        "name": 'metadata',
                        "displayName": '元数据',
                        "type": 'json',
                    },
                    {
                        "name": 'page_content',
                        "displayName": '文本内容',
                        "type": 'string',
                    },
                ],
            },
            {
                "name": "text",
                "displayName": "所有搜索的结果组合的字符串",
                "type": "string"
            }
        ],
        "x-monkey-tool-extra": {
            "estimateTime": 5,
        },
    })
    def post(self, name):
        '''Full Text Search'''
        input_data = request.json
        team_id = request.team_id
        collection_name = input_data.get('collection')
        question = input_data.get('question')
        top_k = input_data.get('topK')
        metadata_filter = input_data.get('metadata_filter', None)

        app_id = request.app_id
        table = CollectionTable(app_id=app_id)
        collection = table.find_by_name(team_id, name=collection_name)
        if not collection:
            raise Exception(f"数据集 {collection_name} 不存在或未授权")

        es_client = ESClient(
            app_id=app_id,
            index_name=collection_name
        )
        embedding_model = collection.get('embeddingModel')
        embedding = generate_embedding_of_model(embedding_model, question)

        data = es_client.vector_search(embedding, top_k, metadata_filter)
        data = [{
            'page_content': item['_source']['page_content'],
            "metadata": item['_source']['metadata']
        } for item in data]
        texts = [
            item['page_content'] for item in data
        ]
        text = '\n'.join(texts)

        return {
            "result": data,
            "text": text
        }
