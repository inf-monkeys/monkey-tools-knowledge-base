from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, JSON, Text, func, select
from datetime import datetime
from src.config import database_url

from src.utils import generate_mongoid

engine = create_engine(database_url, echo=True)
Session = sessionmaker(bind=engine)
session = Session()


class Base(DeclarativeBase):
    pass


def create_collections_model_with_prefix(prefix):
    class DynamicCollectionsModel(Base):
        __tablename__ = f"{prefix}_text_collections"
        __table_args__ = {'extend_existing': True}

        id = Column(String, primary_key=True)
        created_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000))
        updated_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000),
                                   onupdate=int(datetime.now().timestamp() * 1000))
        is_deleted = Column(Boolean, default=False)
        creator_userId = Column(String)
        team_id = Column(String)
        name = Column(String)
        display_name = Column(String)
        description = Column(Text)
        icon_url = Column(String)
        embedding_model = Column(String)
        dimension = Column(Integer)
        asset_type = Column(String, default='text-collection')

        def serialize(self):
            return {
                "id": self.id,
                "createdTimestamp": self.created_timestamp,
                "updatedTimestamp": self.updated_timestamp,
                "isDeleted": self.is_deleted,
                "creatorUserId": self.creator_userId,
                "teamId": self.team_id,
                "name": self.name,
                "displayName": self.display_name,
                "description": self.description,
                "iconUrl": self.icon_url,
                "embeddingModel": self.embedding_model,
                "assetType": self.asset_type
            }

    Base.metadata.create_all(engine)
    return DynamicCollectionsModel


def create_collection_metadata_field_model_with_prefix(prefix):
    class DynamicCollectionMetadataFieldModel(Base):
        __tablename__ = f"{prefix}_text_collection_metadata_fields"
        __table_args__ = {'extend_existing': True}

        id = Column(String, primary_key=True)
        created_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000))
        updated_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000),
                                   onupdate=int(datetime.now().timestamp() * 1000))
        is_deleted = Column(Boolean, default=False)
        collection_name = Column(String)
        key = Column(String)
        display_name = Column(String, nullable=True)
        description = Column(String, nullable=True)
        built_in = Column(Boolean, default=False)
        required = Column(Boolean, default=False)

    Base.metadata.create_all(engine)
    return DynamicCollectionMetadataFieldModel


class CollectionMetadataFieldTable:
    def __init__(self, app_id):
        self.app_id = app_id

    def add_if_not_exists(self, collection_name, key, display_name=None, description=None, built_in=None,
                          required=None):
        model = create_collection_metadata_field_model_with_prefix(self.app_id)
        exists = session.query(model).filter_by(
            collection_name=collection_name,
            key=key,
            is_deleted=False
        ).first()
        if not exists:
            record = model(
                id=generate_mongoid(),
                collection_name=collection_name,
                key=key,
                display_name=display_name,
                description=description,
                built_in=built_in,
                required=required
            )
            session.add(record)
            session.commit()


def create_collection_authorization_model_with_prefix(prefix):
    class DynamicCollectionAuthorizationModel(Base):
        __tablename__ = f"{prefix}_text_collection_authorizations"
        __table_args__ = {'extend_existing': True}

        id = Column(String, primary_key=True)
        created_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000))
        updated_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000),
                                   onupdate=int(datetime.now().timestamp() * 1000))
        is_deleted = Column(Boolean, default=False)
        collection_name = Column(String)
        team_id = Column(String)

    Base.metadata.create_all(engine)
    return DynamicCollectionAuthorizationModel


def create_file_progress_model_with_prefix(prefix):
    class DynamicFileProgressModel(Base):
        __tablename__ = f"{prefix}_file_progress"
        __table_args__ = {'extend_existing': True}

        id = Column(String, primary_key=True)
        created_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000))
        updated_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000),
                                   onupdate=int(datetime.now().timestamp() * 1000))
        is_deleted = Column(Boolean, default=False)
        collection_name = Column(String)
        progress = Column(Integer, nullable=True)
        task_id = Column(String)
        status = Column(String)
        message = Column(String)

        def serialize(self):
            return {
                "createdTimestamp": self.created_timestamp,
                "updatedTimestamp": self.updated_timestamp,
                "progress": self.progress,
                "taskId": self.task_id,
                "status": self.status,
                "message": self.message,
            }

    Base.metadata.create_all(engine)
    return DynamicFileProgressModel


class FileProgressTable:
    def __init__(self, app_id):
        self.app_id = app_id

    def update_progress(self, collection_name, task_id, status, message, progress=None):
        model = create_file_progress_model_with_prefix(self.app_id)
        record = model(
            id=generate_mongoid(),
            collection_name=collection_name,
            task_id=task_id,
            progress=progress,
            status=status,
            message=message,
        )
        session.add(record)
        session.commit()

    def get_task_status(self, task_id):
        model = create_file_progress_model_with_prefix(self.app_id)
        records = session.query(model).filter_by(task_id=task_id).all()
        return records

    def list_tasks(self, collection_name):
        model = create_file_progress_model_with_prefix(self.app_id)
        # 定义窗口函数
        row_number = func.row_number().over(
            partition_by=model.task_id,
            order_by=model.created_timestamp.desc()
        ).label('rn')

        # 构建子查询
        subquery = (
            select(
                model.task_id,
                model.progress,
                model.status,
                model.message,
                row_number
            )
            .where(model.collection_name == collection_name)
            .alias('subquery')
        )
        query = select(subquery).where(subquery.c.rn == 1)
        raw_results = session.execute(query).fetchall()
        data = []
        for item in raw_results:
            data.append({
                "taskId": item[0],
                "progress": item[1],
                "status": item[2],
                "message": item[3]
            })
        return data


def create_file_records_model_with_prefix(prefix):
    class DynamicFileRecordsModel(Base):
        __tablename__ = f"{prefix}_file_records"
        __table_args__ = {'extend_existing': True}

        id = Column(String, primary_key=True)
        created_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000))
        updated_timestamp = Column(Integer, default=int(datetime.now().timestamp() * 1000),
                                   onupdate=int(datetime.now().timestamp() * 1000))
        is_deleted = Column(Boolean, default=False)
        collection_name = Column(String)
        file_url = Column(String)
        split_config = Column(JSON)

    Base.metadata.create_all(engine)
    return DynamicFileRecordsModel


class FileRecordTable:
    def __init__(self, app_id):
        self.app_id = app_id

    def add_record(self, collection_name, file_url, split_config):
        model = create_file_records_model_with_prefix(self.app_id)
        record = model(
            id=generate_mongoid(),
            collection_name=collection_name,
            file_url=file_url,
            split_config=split_config,
        )
        session.add(record)
        session.commit()
