import json
import shutil
import time
import traceback
from typing import List
import uuid

from flask import app
from loguru import logger
from core.middleware.db import db
from core.models.document import Document
from core.middleware.redis_client import redis_client
from core.storage.vectorstore.vector_store_factory import VectorStoreFactory
from core.models.knowledge_base import KnowledgeBaseEntity
from core.utils.oss import download_file, extract_filename
from core.utils.document_loader import load_documents, split_documents
from app import app
from core.models.task import TaskEntity, TaskStatus
from core.models.document import DocumentEntity
from core.utils.zip import extract_files_from_zip
from core.utils.oss.aliyunoss import AliyunOSSClient
from core.utils.oss.tos import TOSClient
from core.models.metadata_field import MetadataFieldEntity


def _download_file(file_url):
    return download_file(file_url)


def _extract_zip(file_url):
    return extract_files_from_zip(file_url)


class OSSReader:
    def __init__(self, oss_type, oss_config) -> None:
        self.oss_type = oss_type
        self.oss_config = oss_config
        self.client = self._init_client()

    def _init_client(self):
        if self.oss_type == "TOS":
            (
                endpoint,
                region,
                bucket_name,
                accessKeyId,
                accessKeySecret,
            ) = (
                self.oss_config.get("endpoint"),
                self.oss_config.get("region"),
                self.oss_config.get("bucketName"),
                self.oss_config.get("accessKeyId"),
                self.oss_config.get("accessKeySecret"),
            )
            if (
                not endpoint
                or not region
                or not bucket_name
                or not accessKeyId
                or not accessKeySecret
            ):
                raise ValueError("Missing TOS configuration")

            return TOSClient(
                endpoint,
                region,
                bucket_name,
                accessKeyId,
                accessKeySecret,
            )
        elif self.oss_type == "ALIYUNOSS":
            (
                endpoint,
                bucket_name,
                accessKeyId,
                accessKeySecret,
            ) = (
                self.oss_config.get("endpoint"),
                self.oss_config.get("bucketName"),
                self.oss_config.get("accessKeyId"),
                self.oss_config.get("accessKeySecret"),
            )
            if (
                not endpoint
                or not bucket_name
                or not accessKeyId
                or not accessKeySecret
            ):
                raise ValueError("Missing AliyunOSS configuration")
            return AliyunOSSClient(
                endpoint=endpoint,
                bucket_name=bucket_name,
                access_key=accessKeyId,
                secret_key=accessKeySecret,
            )
        else:
            raise ValueError(f"Unknown oss type: {self.oss_type}")

    def read_base_folder(self):
        (
            baseFolder,
            fileExtensions,
            excludeFileRegex,
        ) = (
            self.oss_config.get("baseFolder"),
            self.oss_config.get("fileExtensions"),
            self.oss_config.get("excludeFileRegex"),
        )
        if fileExtensions:
            fileExtensions = fileExtensions.split(",")
        return self.client.read_base_folder(
            baseFolder, fileExtensions, excludeFileRegex
        )

    def get_signed_url(self, key, expires=3600):
        return self.client.get_signed_url(key, expires)


def _extract_documents(
    file_path: str,
    pre_process_rules,
    jqSchema,
):
    return load_documents(
        file_path,
        pre_process_rules=pre_process_rules,
        jqSchema=jqSchema,
    )


def _split_documents(
    documents: List[Document],
    chunk_size,
    chunk_overlap,
    separator,
    jqSchema,
):
    return split_documents(
        documents,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separator=separator,
        jqSchema=jqSchema,
    )


def _load_single_document(
    knowledge_base_id,
    user_id,
    filename,
    file_url,
    file_path,
    pre_process_rules,
    jqSchema,
    chunk_size,
    chunk_overlap,
    separator,
    on_prgress,
):
    with app.app_context():
        try:
            knowledge_base = KnowledgeBaseEntity.get_by_id(knowledge_base_id)
            vector_store = VectorStoreFactory(knowledge_base)

            # Save document to database
            document_id = str(uuid.uuid4())
            document_entity = DocumentEntity(
                id=document_id,
                knowledge_base_id=knowledge_base_id,
                index_status=TaskStatus.PENDING.value,
                filename=filename,
                file_url=file_url,
            )
            db.session.add(document_entity)

            documents = _extract_documents(
                file_path,
                pre_process_rules=pre_process_rules,
                jqSchema=jqSchema,
            )

            on_prgress(
                TaskStatus.IN_PROGRESS, f"Extracted {len(documents)} documents", 0.3
            )

            splitted_segments = _split_documents(
                documents,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separator=separator,
                jqSchema=jqSchema,
            )

            on_prgress(
                TaskStatus.IN_PROGRESS,
                f"Splitted to {len(splitted_segments)} segments",
                0.5,
            )

            metadata_fields = set()
            for i, segment in enumerate(splitted_segments):
                if not segment.metadata:
                    segment.metadata = {}
                if segment.metadata.get("source"):
                    del segment.metadata["source"]
                segment.metadata["filename"] = filename
                segment.metadata["created_at"] = int(time.time())
                segment.metadata["document_id"] = document_id
                segment.metadata["user_id"] = user_id

                metadata_fields.update(segment.metadata.keys())

            vector_store.add_texts(splitted_segments)
            MetadataFieldEntity.add_keys_if_not_exists(
                knowledge_base_id, metadata_fields
            )

            on_prgress(TaskStatus.COMPLETED, "Loaded to vector store", 1)
            DocumentEntity.update_status_by_id(
                document_id,
                index_status=TaskStatus.COMPLETED,
                failed_message=None,
            )
            return True
        except Exception as e:
            on_prgress(TaskStatus.FAILED, f"{str(e)}")
            DocumentEntity.update_status_by_id(
                document_id,
                index_status=TaskStatus.FAILED,
                failed_message=str(e),
            )
            logger.error(f"Failed to process task: {str(e)}")
            traceback.print_exc()
            return False


def consume_task(task_data):
    task_id = task_data["task_id"]
    knowledge_base_id = task_data["knowledge_base_id"]

    user_id = task_data["user_id"]

    # Split config
    chunk_size = task_data["chunk_size"]
    chunk_overlap = task_data["chunk_overlap"]
    separator = task_data["separator"]
    pre_process_rules = task_data["pre_process_rules"]
    jqSchema = task_data["jqSchema"]

    # File config
    file_url = task_data["file_url"]
    filename = task_data["filename"]

    # Oss config
    oss_type = task_data["oss_type"]
    oss_config = task_data["oss_config"]

    with app.app_context():
        try:

            def on_prgress(status, latest_message, progress=None):
                TaskEntity.update_progress_by_id(
                    task_id,
                    status=status,
                    progress=progress,
                    latest_message=latest_message,
                )

            if file_url and file_url.endswith(".zip"):
                extract_to, files = _extract_zip(file_url)
                on_prgress(TaskStatus.IN_PROGRESS, "Downloaded file And Extracted", 0.1)
                succeed = 0
                failed = 0
                total = len(files)
                for file_path in files:
                    success = _load_single_document(
                        knowledge_base_id,
                        user_id,
                        extract_filename(file_path),
                        file_url,
                        file_path,
                        pre_process_rules,
                        jqSchema,
                        chunk_size,
                        chunk_overlap,
                        separator,
                        lambda status, progress, latest_message: None,
                    )
                    if success:
                        succeed += 1
                    else:
                        failed += 1

                    progress = 0.1 + 0.9 * ((succeed + failed) / total)
                    on_prgress(
                        TaskStatus.IN_PROGRESS,
                        f"Succeed {succeed}/{total}, Failed {failed}/{total}",
                        progress,
                    )
                on_prgress(TaskStatus.COMPLETED, "Loaded all documents", 1)
                shutil.rmtree(extract_to)
            elif oss_type and oss_config:
                oss_reader = OSSReader(oss_type, oss_config)
                files = oss_reader.read_base_folder()
                on_prgress(
                    TaskStatus.IN_PROGRESS, f"Read {len(files)} files in OSS", 0.1
                )
                succeed = 0
                failed = 0
                total = len(files)
                for file in files:
                    logger.info(f"Processing file: {file}")
                    signed_url = oss_reader.get_signed_url(file)
                    logger.info(f"Download file from: {signed_url}")
                    file_path = _download_file(signed_url)
                    logger.info(f"Downloaded file to: {file_path}")
                    success = _load_single_document(
                        knowledge_base_id,
                        user_id,
                        extract_filename(file),
                        signed_url,
                        file_path,
                        pre_process_rules,
                        jqSchema,
                        chunk_size,
                        chunk_overlap,
                        separator,
                        lambda status, progress, latest_message: None,
                    )
                    if success:
                        succeed += 1
                    else:
                        failed += 1

                    progress = 0.1 + 0.9 * ((succeed + failed) / total)
                    on_prgress(
                        TaskStatus.IN_PROGRESS,
                        f"Succeed {succeed}/{total}, Failed {failed}/{total}",
                        progress,
                    )
                on_prgress(TaskStatus.COMPLETED, "Loaded all documents", 1)

            elif file_url:
                file_path = _download_file(file_url)
                if not file_path:
                    raise ValueError("Failed to download file")
                on_prgress(TaskStatus.IN_PROGRESS, "Downloaded file", 0.1)
                _load_single_document(
                    knowledge_base_id,
                    user_id,
                    filename,
                    file_url,
                    file_path,
                    pre_process_rules,
                    jqSchema,
                    chunk_size,
                    chunk_overlap,
                    separator,
                    on_prgress,
                )

            else:
                raise ValueError("Invalid task data")
        except Exception as e:
            TaskEntity.update_progress_by_id(
                task_id,
                status=TaskStatus.FAILED,
                latest_message=f"Failed to process task: {str(e)}",
            )
            logger.error(f"Failed to process task: {task_data}")
            traceback.print_exc()


# 从队列中获取并处理任务
def consume_task_forever(queue_name):
    logger.info(f"Start consuming tasks from queue: {queue_name}")
    while True:
        try:
            # 使用 blpop 阻塞等待任务
            _, task_json_str = redis_client.blpop(queue_name)
            task_data = json.loads(task_json_str)
            logger.info(f"Processing task: {task_data}")
            consume_task(task_data)
        except Exception as e:
            logger.error(f"Failed to process task: {task_data}")
            traceback.print_exc()
