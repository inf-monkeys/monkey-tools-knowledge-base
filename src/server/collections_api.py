import uuid

from .app import api
from flask import request, jsonify
from flask_restx import Resource
from src.database import create_collections_model_with_prefix, session, \
    create_collection_authorization_model_with_prefix, FileProgressTable, CollectionMetadataFieldTable

from src.es import ESClient
from src.queue import submit_task, PROCESS_FILE_QUEUE_NAME
from src.utils import generate_random_string, generate_short_id, generate_md5, generate_mongoid
from src.utils.embedding import get_dimension_by_embedding_model, generate_embedding_of_model

collection_ns = api.namespace('collections', description='Collection operations')


def get_collection_or_fail(app_id, team_id, name):
    model = create_collections_model_with_prefix(app_id)
    collection = session.query(model).filter_by(name=name, team_id=team_id, is_deleted=False).first()
    if not collection:
        raise Exception(f"Collection {name} not exists")
    return collection


@collection_ns.route('/')
class CollectionList(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('list_collections')
    def get(self):
        '''List all Collections'''
        team_id = request.team_id
        app_id = request.app_id
        collection_model = create_collections_model_with_prefix(app_id)
        collection_authorization_model = create_collection_authorization_model_with_prefix(app_id)
        team_owned_records = session.query(collection_model).filter_by(team_id=team_id, is_deleted=False).all()
        team_authorized_records = session.query(collection_authorization_model).filter_by(team_id=team_id).all()
        # file_record_table = FileRecord(app_id=app_id)
        data = []
        for item in team_owned_records:
            # TODO: add doc count and file count
            data.append(item.serialize())

        authorized_collection_names = []
        for item in team_authorized_records:
            authorized_collection_names.append(item.collection_name)
        if len(authorized_collection_names) > 0:
            team_authorized_collections = session.query(collection_model).filter(
                collection_model.name.in_(authorized_collection_names),
                collection_model.is_deleted == False
            )
            for item in team_authorized_collections:
                # TODO: add doc count and file count
                data.append(item.serialize())

        return jsonify({
            "list": data
        })

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('create_collection')
    def post(self):
        '''Create a new Collection'''
        app_id = request.app_id
        user_id = request.user_id
        team_id = request.team_id

        data = request.json
        display_name = data.get('displayName')
        icon_url = data.get('iconUrl')
        name = generate_random_string()
        embedding_model = data.get('embeddingModel')
        description = data.get('description', '')
        dimension = get_dimension_by_embedding_model(embedding_model)
        model = create_collections_model_with_prefix(app_id)

        collection_entity = model(
            id=generate_mongoid(),
            creator_userId=user_id,
            team_id=team_id,
            name=name,
            display_name=display_name,
            description=description,
            icon_url=icon_url,
            embedding_model=embedding_model,
            dimension=dimension,
        )
        session.add(collection_entity)
        session.commit()

        # 在 es 中创建 template
        es_client = ESClient(
            app_id=app_id,
            index_name=name
        )
        es_client.create_es_index(dimension)
        return {
            "success": True,
            "name": name
        }


@collection_ns.route('/<string:name>')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionDetail(Resource):
    '''Show a single todo item and lets you delete them'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('get_collection')
    def get(self, name):
        '''Fetch a given collection'''
        team_id = request.team_id
        app_id = request.app_id
        collection = get_collection_or_fail(app_id, team_id, name)
        return jsonify(collection.serialize())

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('delete_collection')
    @collection_ns.response(204, 'Collection deleted')
    def delete(self, name):
        '''Delete a collection given its identifier'''
        team_id = request.team_id
        app_id = request.app_id
        model = create_collections_model_with_prefix(app_id)
        session.query(model).filter_by(team_id=team_id, name=name).update({"is_deleted": True})
        session.commit()
        es_client = ESClient(app_id=app_id, index_name=name)
        es_client.delete_index()
        return {
            "success": True
        }

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('update_collection')
    @collection_ns.response(201, 'Collection updated')
    def put(self, name):
        '''Update a collection given its identifier'''
        team_id = request.team_id
        app_id = request.app_id
        model = create_collections_model_with_prefix(app_id)
        collection = get_collection_or_fail(app_id, team_id, name)
        data = request.json
        description = data.get('description')
        display_name = data.get('displayName')
        icon_url = data.get('iconUrl')
        session.query(model).filter_by(name=name, is_deleted=False).update({
            "description": description or collection.description,
            "display_name": display_name or collection.display_name,
            "icon_url": icon_url or collection.icon_url
        })
        return {
            "success": True
        }


@collection_ns.route('/<string:name>/data')
@collection_ns.param('name', 'The collection identifier')
class CollectionData(Resource):
    '''Show a single todo item and lets you delete them'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('delete_collection_data')
    def delete(self, name):
        '''Delete data in a collection'''
        app_id = request.app_id
        team_id = request.team_id
        collection = get_collection_or_fail(app_id, team_id, name)
        es_client = ESClient(app_id=app_id, index_name=name)
        # 删除索引
        es_client.delete_index()
        es_client.create_es_index(
            dimension=collection.dimension
        )
        return {
            "success": True
        }


@collection_ns.route('/<string:name>/authorization')
@collection_ns.param('name', 'The collection identifier')
class CollectionAuthorization(Resource):
    '''Show a single todo item and lets you delete them'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('authorize_collection')
    def put(self, name):
        '''Authorize collection to other team or user'''
        app_id = request.app_id
        team_id = request.team_id
        get_collection_or_fail(app_id, team_id, name)
        data = request.json
        team_id = data.get('teamId')

        model = create_collection_authorization_model_with_prefix(app_id)
        record = model(
            id=generate_mongoid(),
            collection_name=name,
            team_id=team_id
        )
        session.add(record)
        session.commit()

        return {
            "success": True
        }


@collection_ns.route('/<string:name>/copy')
@collection_ns.param('name', 'The collection identifier')
class CollectionCopy(Resource):
    '''Show a single todo item and lets you delete them'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('copy_collection')
    def post(self, name):
        """Copy a collection"""
        app_id = request.app_id
        team_id = request.team_id
        collection = get_collection_or_fail(app_id, team_id, name)
        data = request.json
        team_id = data.get('teamId')
        user_id = data.get('userId')

        embedding_model = collection.embedding_model
        dimension = collection.dimension
        new_collection_name = generate_short_id()
        description = collection.description

        # 在 es 中创建 template
        es_client = ESClient(app_id=app_id, index_name=new_collection_name)
        es_client.create_es_index(
            dimension
        )
        model = create_collections_model_with_prefix(app_id)
        collection_entity = model(
            id=generate_mongoid(),
            creator_userId=user_id,
            team_id=team_id,
            name=new_collection_name,
            display_name=collection.display_name,
            description=description,
            icon_url=collection.icon_url,
            embedding_model=embedding_model,
            dimension=dimension,
            metadata_fields=collection.metadata_fields,
        )
        session.add(collection_entity)
        session.commit()
        return {
            "name": new_collection_name
        }


@collection_ns.route('/<string:name>/tasks')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class TaskList(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('list_tasks')
    def get(self, name):
        '''List all Tasks'''
        team_id = request.team_id
        app_id = request.app_id
        table = FileProgressTable(
            app_id=app_id
        )
        data = table.list_tasks(
            collection_name=name
        )
        return jsonify({
            "list": data
        })

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('create_task')
    def post(self, name):
        '''Create A Task'''
        team_id = request.team_id
        user_id = request.user_id
        app_id = request.app_id

        collection = get_collection_or_fail(app_id, team_id, name)
        embedding_model = collection.embedding_model

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

        progress_table = FileProgressTable(app_id=app_id)
        progress_table.update_progress(
            collection_name=name,
            task_id=task_id,
            status="PENDING",
            message="Added to queue",
            progress=0
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

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('get_task_detail')
    def get(self, name, task_id):
        '''Get A Task Detail'''
        team_id = request.team_id
        app_id = request.app_id
        table = FileProgressTable(
            app_id=app_id
        )
        records = table.get_task_status(task_id=task_id)
        return jsonify([record.serialize() for record in records])


@collection_ns.route('/<string:name>/vectors')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectors(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('create_vector')
    def post(self, name):
        '''Create A Vector'''
        team_id = request.team_id
        user_id = request.user_id
        app_id = request.app_id
        collection = get_collection_or_fail(app_id, team_id, name)
        embedding_model = collection.embedding_model
        data = request.json
        text = data.get("text")
        if not text:
            raise Exception("text in empty")
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

            for key in metadata.keys():
                metadata_fields_table = CollectionMetadataFieldTable(app_id=app_id)
                metadata_fields_table.add_if_not_exists(
                    collection_name=name,
                    key=key
                )
            return {
                "pk": pk
            }


@collection_ns.route('/<string:name>/vectors/<string:pk>')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectorDetail(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('delete_vector')
    def delete(self, name, pk):
        '''Create A Vector'''
        app_id = request.app_id
        es_client = ESClient(app_id=app_id, index_name=name)
        es_client.delete_es_document(pk)
        return {"success": True}

    @collection_ns.doc('upsert_vector')
    def put(self, name, pk):
        '''Create A Vector'''
        data = request.json
        team_id = request.team_id
        app_id = request.app_id
        text = data.get("text")
        if not text:
            raise Exception("text is empty")
        metadata = data.get("metadata")
        collection = get_collection_or_fail(app_id, team_id, name)
        embedding_model = collection.embedding_model
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
        return {"success": True}


@collection_ns.route('/<string:name>/vectors-batch')
@collection_ns.response(404, 'Collection not found')
@collection_ns.param('name', 'The collection identifier')
class CollectionVectorBatch(Resource):
    '''Shows a list of all todos, and lets you POST to add new tasks'''

    @api.vendor({
        "x-monkey-tool-ignore": True,
    })
    @collection_ns.doc('upsert_vectors')
    def put(self, name):
        '''Create A Vector'''
        app_id = request.app_id
        team_id = request.team_id
        collection = get_collection_or_fail(app_id=app_id, team_id=team_id, name=name)
        embedding_model = collection.embedding_model
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
                    "metadata": item.get('metadata', {}),
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
        question = input_data.get('question')
        if not question:
            raise Exception("question is empty")
        top_k = input_data.get('topK', 3)
        metadata_filter = input_data.get('metadata_filter', None)

        app_id = request.app_id
        collection = get_collection_or_fail(app_id, team_id, name)

        es_client = ESClient(
            app_id=app_id,
            index_name=name
        )
        embedding_model = collection.embedding_model
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
