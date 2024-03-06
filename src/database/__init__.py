import time
import os

from pymongo import MongoClient

MONGO_URL = os.environ.get("MONGO_URL")

client = MongoClient(MONGO_URL)
db = client.vines


class CollectionTable:

    def __init__(self, app_id):
        self.app_id = app_id
        self.collection = db[self.app_id + "-" + "assets-text-collections"]

    def insert_one(
            self,
            creator_user_id,
            team_id,
            name,
            display_name,
            description,
            logo,
            embedding_model,
            dimension,
            metadata_fields
    ):
        timestamp = int(time.time())
        metadata_fields = metadata_fields or [
            {
                "name": 'userId',
                "displayName": '创建此向量的用户 ID',
                "description": '创建此向量的用户 ID',
                "builtIn": True,
                "required": True,
            },
            {
                "name": 'workflowId',
                "displayName": '工作流 ID',
                "description": '当此向量是通过工作流创建的时候会包含，为创建此向量的工作流 ID',
                "builtIn": True,
                "required": False,
            },
            {
                "name": 'createdAt',
                "displayName": '创建时间',
                "description": 'Unix 时间戳',
                "builtIn": True,
                "required": True,
            },
            {
                "name": 'fileUrl',
                "displayName": '来源文件链接',
                "description": '当通过文件导入时会包含此值，为来源文件的链接',
                "builtIn": True,
                "required": False,
            },
        ]
        return self.collection.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "creatorUserId": creator_user_id,
            "teamId": team_id,
            "name": name,
            "displayName": display_name,
            "description": description,
            "logo": logo,
            "embeddingModel": embedding_model,
            "dimension": dimension,
            "metadataFields": metadata_fields,
            "authorizedTargets": [],
            "assetType": "text-collection"
        })

    def check_name_conflicts(self, name):
        record = self.collection.find_one({
            "name": name,
            "isDeleted": False
        })
        return bool(record)

    def find_by_team(self, team_id):
        return self.collection.find({
            "$or": [
                {
                    "teamId": team_id,
                    "isDeleted": False,
                },
                {
                    "authorizedTargets": {
                        "$elemMatch": {
                            "targetType": "TEAM",
                            "targetId": team_id
                        }
                    }
                }
            ]
        }).sort("_id", -1)

    def find_by_name(self, team_id, name):
        return self.collection.find_one({
            "$or": [
                {
                    "isDeleted": False,
                    "name": name,
                    "teamId": team_id,
                },
                {
                    "isDeleted": False,
                    "name": name,
                    "authorizedTargets": {
                        "$elemMatch": {
                            "targetType": "TEAM",
                            "targetId": team_id
                        }
                    }
                }
            ]
        })

    def find_by_name_without_team(self, name):
        return self.collection.find_one({
            "isDeleted": False,
            "name": name
        })

    def update_by_name(self, team_id, name, description=None, display_name=None, logo=None, new_name=None):
        updates = {}
        if description:
            updates['description'] = description
        if display_name:
            updates['displayName'] = display_name
        if logo:
            updates['logo'] = logo
        if new_name:
            updates['name'] = new_name
        updates['updatedTimestamp'] = int(time.time())
        return self.collection.update_one(
            {
                "teamId": team_id,
                "isDeleted": False,
                "name": name
            },
            {
                "$set": updates
            }
        )

    def delete_by_name(self, team_id, name):
        return self.collection.update_one(
            {
                "teamId": team_id,
                "isDeleted": False,
                "name": name
            },
            {
                "$set": {
                    "isDeleted": True
                }
            }
        )

    def authorize_target(self, name: str, team_id: str):
        return self.collection.update_one(
            {
                "isDeleted": False,
                "name": name
            },
            {
                "$push": {
                    "authorizedTargets": {
                        "targetType": "TEAM",
                        "targetId": team_id
                    }
                }
            }
        )

    def add_metadata_fields_if_not_exists(self, team_id, coll_name, field_names):
        coll = self.find_by_name(team_id, coll_name)
        fields_to_add = []
        for field_name in field_names:
            not_exist = len(list(filter(lambda x: x['name'] == field_name, coll['metadataFields']))) == 0
            if not_exist:
                fields_to_add.append(field_name)
        if len(fields_to_add) == 0:
            return
        for field_name in fields_to_add:
            self.collection.update_one(
                {
                    "teamId": team_id,
                    "isDeleted": False,
                    "name": coll_name
                },
                {
                    "$push": {
                        "metadataFields": {
                            "name": field_name,
                            "displayName": '',
                            "description": '',
                            "builtIn": False,
                            "required": False,
                        }
                    }
                }
            )


class AccountTable:

    def __init__(self, app_id):
        self.app_id = app_id
        self.collection = db[self.app_id + "-" + "vector-accounts"]

    def find_by_team_id(self, team_id):
        return self.collection.find_one({
            "teamId": team_id,
            "isDeleted": False,
        })

    def create_user(self, team_id, role_name, username, password):
        timestamp = int(time.time())
        self.collection.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "teamId": team_id,
            "roleName": role_name,
            "username": username,
            "password": password
        })
        return self.find_by_team_id(team_id)

    def find_or_create(self, team_id, role_name, username, password):
        entity = self.find_by_team_id(team_id)
        if entity:
            return entity
        return self.create_user(team_id, role_name, username, password)


class FileRecord:
    def __init__(self, app_id):
        self.app_id = app_id
        self.collection = db[self.app_id + "-" + "vector-file-records"]

    def create_record(self, team_id, collection_name, file_url, split_config):
        timestamp = int(time.time())
        self.collection.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "teamId": team_id,
            "collectionName": collection_name,
            "fileUrl": file_url,
            "splitConfig": split_config
        })

    def get_file_count(self, team_id, collection_name):
        return self.collection.count_documents({
            "teamId": team_id,
            "isDeleted": False,
            "collectionName": collection_name
        })


class FileProcessProgressTable:

    def __init__(self, app_id):
        self.app_id = app_id
        self.collection = db[self.app_id + "-" + "vector-file-process-progress"]

    def list_tasks(self, team_id, collection_name):
        return self.collection.find(
            {
                "teamId": team_id,
                "collectionName": collection_name
            }
        ).sort("_id", -1)

    def get_task(self, team_id, collection_name, task_id):
        return self.collection.find_one({
            "teamId": team_id,
            "collectionName": collection_name,
            "taskId": task_id,
            "isDeleted": False,
        })

    def create_task(self, team_id, collection_name, task_id):
        timestamp = int(time.time())
        self.collection.insert_one({
            "createdTimestamp": timestamp,
            "updatedTimestamp": timestamp,
            "isDeleted": False,
            "teamId": team_id,
            "collectionName": collection_name,
            "taskId": task_id,
            "events": []
        })

    def mark_task_failed(self, task_id, message):
        self.collection.update_one(
            {
                "taskId": task_id
            },
            {
                "$push": {
                    "events": {
                        "message": message,
                        "timestamp": int(time.time()),
                        "status": "FAILED"
                    }
                }
            }
        )

    def update_progress(self, task_id, progress, message):
        status = "INPROGRESS" if progress < 1 else "COMPLETED"
        self.collection.update_one(
            {
                "taskId": task_id
            },
            {
                "$push": {
                    "events": {
                        "progress": progress,
                        "message": message,
                        "timestamp": int(time.time()),
                        "status": status
                    }
                }
            }
        )
