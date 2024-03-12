import redis
import json
import traceback
from src.utils.oss.tos import TOSClient
from src.utils.oss.aliyunoss import AliyunOSSClient
from src.database import FileProgressTable, CollectionMetadataFieldTable
from src.es import ESClient
from src.config import config_data

redis_config = config_data.get('redis', {})

REDIS_URL = redis_config.get('url')
redis = redis.from_url(REDIS_URL)

PROCESS_FILE_QUEUE_NAME = 'queue:monkey-tools-vector:process-file'


def submit_task(queue_name, task_data):
    task_json = json.dumps(task_data)
    redis.rpush(queue_name, task_json)


def consume_task(task_data):
    app_id = task_data['app_id']
    team_id = task_data['team_id']
    collection_name = task_data['collection_name']
    embedding_model = task_data['embedding_model']
    file_url = task_data['file_url']
    oss_config = task_data['oss_config']
    metadata = task_data['metadata']
    task_id = task_data['task_id']
    chunk_size = task_data['chunk_size']
    chunk_overlap = task_data['chunk_overlap']
    separator = task_data['separator']
    pre_process_rules = task_data['pre_process_rules']
    jqSchema = task_data['jqSchema']
    es_client = ESClient(app_id=app_id, index_name=collection_name)
    progress_table = FileProgressTable(app_id)
    metadata_field_table = CollectionMetadataFieldTable(app_id=app_id)

    # 如果是通过 oss 导入，先获取链接，然后再写入消息队列
    if oss_config:
        try:
            oss_type, oss_config = oss_config.get('ossType'), oss_config.get('ossConfig')
            if oss_type == 'TOS':
                endpoint, region, bucket_name, accessKeyId, accessKeySecret, baseFolder, fileExtensions, excludeFileRegex, importFileNameNotContent = oss_config.get(
                    'endpoint'), oss_config.get('region'), oss_config.get('bucketName'), oss_config.get(
                    'accessKeyId'), oss_config.get('accessKeySecret'), oss_config.get('baseFolder'), oss_config.get(
                    'fileExtensions'), oss_config.get('excludeFileRegex'), oss_config.get(
                    'importFileNameNotContent')
                if fileExtensions:
                    fileExtensions = fileExtensions.split(',')
                tos_client = TOSClient(
                    endpoint,
                    region,
                    bucket_name,
                    accessKeyId,
                    accessKeySecret,
                )
                all_files = tos_client.get_all_files_in_base_folder(
                    baseFolder,
                    fileExtensions,
                    excludeFileRegex
                )
                progress_table.update_progress(
                    collection_name=collection_name,
                    task_id=task_id, progress=0.1,
                    status="IN_PROGRESS",
                    message=f"共获取到 {len(all_files)} 个文件"
                )
                processed = 0
                failed = 0
                for absolute_filename in all_files:
                    try:
                        presign_url = tos_client.get_signed_url(absolute_filename)
                        signed_url = presign_url.signed_url
                        metadata = {
                            "filename": absolute_filename
                        }
                        es_client.insert_vector_from_file(
                            team_id,
                            embedding_model, signed_url, metadata,
                            chunk_size=chunk_size,
                            chunk_overlap=chunk_overlap,
                            separator=separator,
                            pre_process_rules=pre_process_rules,
                            jqSchema=jqSchema
                        )
                    except Exception as e:
                        failed += 1
                        print(f"导入文件失败：file={absolute_filename}, 错误信息: ")
                        traceback.print_exc()
                    finally:
                        processed += 1
                        progress = "{:.2f}".format(processed / len(all_files))
                        message = f"已成功写入 {processed}/{len(all_files)} 个文件" if failed == 0 else f"已成功写入 {processed}/{len(all_files)} 个文件，失败 {failed} 个文件"
                        progress_table.update_progress(
                            collection_name=collection_name,
                            task_id=task_id, progress=0.1 + float(progress),
                            message=message,
                            status="IN_PROGRESS"
                        )

                metadata_field_table.add_if_not_exists(collection_name, key="filename")
                metadata_field_table.add_if_not_exists(collection_name, key="filepath")

            elif oss_type == 'ALIYUNOSS':
                endpoint, bucket_name, accessKeyId, accessKeySecret, baseFolder, fileExtensions, excludeFileRegex, importFileNameNotContent = oss_config.get(
                    'endpoint'), oss_config.get('bucketName'), oss_config.get(
                    'accessKeyId'), oss_config.get('accessKeySecret'), oss_config.get('baseFolder'), oss_config.get(
                    'fileExtensions'), oss_config.get('excludeFileRegex'), oss_config.get(
                    'importFileNameNotContent')
                aliyunoss_client = AliyunOSSClient(
                    endpoint=endpoint,
                    bucket_name=bucket_name,
                    access_key=accessKeyId,
                    secret_key=accessKeySecret
                )
                all_files = aliyunoss_client.get_all_files_in_base_folder(
                    baseFolder,
                    fileExtensions,
                    excludeFileRegex
                )
                progress_table.update_progress(
                    collection_name=collection_name,
                    task_id=task_id, progress=0.1, message=f"共获取到 {len(all_files)} 个文件",
                    status="IN_PROGRESS"
                )
                processed = 0
                failed = 0
                for absolute_filename in all_files:
                    try:
                        signed_url = aliyunoss_client.get_signed_url(absolute_filename)
                        metadata = {
                            "filename": absolute_filename
                        }
                        es_client.insert_vector_from_file(
                            team_id,
                            embedding_model, signed_url, metadata,
                            chunk_size=chunk_size,
                            chunk_overlap=chunk_overlap,
                            separator=separator,
                            pre_process_rules=pre_process_rules,
                            jqSchema=jqSchema
                        )
                    except Exception as e:
                        failed += 1
                        print(f"导入文件失败：file={absolute_filename}, 错误信息: ")
                        traceback.print_exc()
                    finally:
                        processed += 1
                        progress = "{:.2f}".format(processed / len(all_files))
                        message = f"已成功写入 {processed}/{len(all_files)} 个文件" if failed == 0 else f"已成功写入 {processed}/{len(all_files)} 个文件，失败 {failed} 个文件"
                        progress_table.update_progress(
                            collection_name=collection_name,
                            task_id=task_id, progress=0.1 + float(progress),
                            message=message,
                            status="IN_PROGRESS"
                        )

                metadata_field_table.add_if_not_exists(collection_name, key="filename")
                metadata_field_table.add_if_not_exists(collection_name, key="filepath")


        except Exception as e:
            traceback.print_exc()
            progress_table.update_progress(
                collection_name=collection_name,
                task_id=task_id, message=str(e),
                status="FAILED"
            )

    elif file_url:
        try:
            es_client.insert_vector_from_file(
                team_id,
                embedding_model, file_url, metadata, task_id,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separator=separator,
                pre_process_rules=pre_process_rules,
                jqSchema=jqSchema
            )
            for key in metadata.keys():
                metadata_field_table.add_if_not_exists(collection_name, key=key)
        except Exception as e:
            traceback.print_exc()
            progress_table.update_progress(
                task_id=task_id, message=str(e),
                collection_name=collection_name,
                status="FAILED"
            )


# 从队列中获取并处理任务
def consume_task_forever(queue_name):
    while True:
        try:
            # 使用 blpop 阻塞等待任务
            _, task_json_str = redis.blpop(queue_name)
            task_data = json.loads(task_json_str)
            print(f"Processing task: {task_data}")
            consume_task(task_data)
        except Exception as e:
            print("消费任务失败：")
            print("=============================")
            print(e)
            print("=============================")
