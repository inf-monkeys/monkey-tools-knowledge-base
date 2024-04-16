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


def _download_file(file_url):
    return download_file(file_url)


def _extract(
    file_path: str,
    pre_process_rules,
    jqSchema,
):
    return load_documents(
        file_path,
        pre_process_rules=pre_process_rules,
        jqSchema=jqSchema,
    )


def _split(
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

            documents = _extract(
                file_path,
                pre_process_rules=pre_process_rules,
                jqSchema=jqSchema,
            )

            on_prgress(
                TaskStatus.IN_PROGRESS, 0.3, f"Extracted {len(documents)} documents"
            )

            splitted_segments = _split(
                documents,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separator=separator,
                jqSchema=jqSchema,
            )

            on_prgress(
                TaskStatus.IN_PROGRESS,
                0.5,
                f"Splitted to {len(splitted_segments)} segments",
            )

            for i, segment in enumerate(splitted_segments):
                if not segment.metadata:
                    segment.metadata = {}
                if segment.metadata.get("source"):
                    del segment.metadata["source"]
                segment.metadata["filename"] = filename
                segment.metadata["created_at"] = int(time.time())
                segment.metadata["document_id"] = document_id

            vector_store.save_documents(splitted_segments)

            on_prgress(TaskStatus.COMPLETED, 1, "Loaded to vector store")
            DocumentEntity.update_status_by_id(
                document_id,
                index_status=TaskStatus.COMPLETED,
                failed_message=None,
            )
            return True
        except Exception as e:
            on_prgress(TaskStatus.FAILED, 1, f"{str(e)}")
            DocumentEntity.update_status_by_id(
                document_id,
                index_status=TaskStatus.FAILED,
                failed_message=str(e),
            )
            logger.error(f"Failed to process task: {str(e)}")
            traceback.print_exc()
            return False


def consume_task(task_data):
    knowledge_base_id = task_data["knowledge_base_id"]
    file_url = task_data["file_url"]
    filename = task_data["filename"]
    task_id = task_data["task_id"]
    chunk_size = task_data["chunk_size"]
    chunk_overlap = task_data["chunk_overlap"]
    separator = task_data["separator"]
    pre_process_rules = task_data["pre_process_rules"]
    jqSchema = task_data["jqSchema"]

    with app.app_context():
        try:
            def on_prgress(status, progress, latest_message):
                TaskEntity.update_progress_by_id(
                    task_id,
                    status=status,
                    progress=progress,
                    latest_message=latest_message,
                )

            if file_url.endswith(".zip"):
                extract_to, files = extract_files_from_zip(file_url)
                on_prgress(TaskStatus.IN_PROGRESS, 0.1, "Downloaded file And Extracted")
                succeed = 0
                failed = 0
                total = len(files)
                for file_path in files:
                    success = _load_single_document(
                        knowledge_base_id,
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
                        progress,
                        f"Succeed {succeed}/{total}, Failed {failed}/{total}",
                    )
                on_prgress(TaskStatus.COMPLETED, 1, "Loaded all documents")
                shutil.rmtree(extract_to)

            else:
                file_path = _download_file(file_url)
                on_prgress(TaskStatus.IN_PROGRESS, 0.1, "Downloaded file")
                _load_single_document(
                    knowledge_base_id,
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
        except Exception as e:
            TaskEntity.update_progress_by_id(
                task_id,
                status=TaskStatus.FAILED,
                progress=1,
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
