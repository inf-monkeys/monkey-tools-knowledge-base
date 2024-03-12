import uuid
import os
from FlagEmbedding import FlagModel
from random import choice
from string import ascii_letters
from shortid import ShortId
import torch
import re
import hashlib
from bson.objectid import ObjectId

sid = ShortId()

ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def generate_pk():
    return str(uuid.uuid4())


def generate_short_id():
    return str(sid.generate())


def generate_random_string(length=12):
    return ''.join(choice(ascii_letters) for i in range(length))


def generate_md5(string: str):
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def generate_embedding_of_model(model_name, q):
    model_path = get_model_path_by_embedding_model(model_name)
    model = FlagModel(
        # 如果本地有下载 model 使用本地的，否则在线下载
        model_path if os.path.exists(model_path) else model_name,
        use_fp16=True
    )  # Setting use_fp16 to True speeds up computation with a slight performance degradation
    embeddings = model.encode(q)
    torch.cuda.empty_cache()
    return embeddings


SUPPORTED_EMBEDDING_MODELS = [
    {
        "name": "BAAI/bge-base-zh-v1.5",
        "displayName": "BAAI/bge-base-zh-v1.5",
        "dimension": 768,
        "enabled": True,
        "link": "https://huggingface.co/BAAI/bge-base-zh-v1.5",
        "model_path": os.path.join(ROOT_FOLDER, "models/bge-base-zh-v1.5")
    },
    {
        "name": "jinaai/jina-embeddings-v2-base-en",
        "displayName": "jinaai/jina-embeddings-v2-base-en",
        "dimension": 768,
        "enabled": True,
        "link": "https://huggingface.co/jinaai/jina-embeddings-v2-base-en",
        "model_path": os.path.join(ROOT_FOLDER, "models/jina-embeddings-v2-base-en")
    },
    {
        "name": "jinaai/jina-embeddings-v2-small-en",
        "displayName": "jinaai/jina-embeddings-v2-small-en",
        "dimension": 768,
        "enabled": True,
        "link": "https://huggingface.co/jinaai/jina-embeddings-v2-small-en",
        "model_path": os.path.join(ROOT_FOLDER, "models/jina-embeddings-v2-small-en")
    },
    {
        "name": "moka-ai/m3e-base",
        "displayName": "moka-ai/m3e-base",
        "dimension": 768,
        "enabled": True,
        "link": "https://huggingface.co/moka-ai/m3e-base",
        "model_path": os.path.join(ROOT_FOLDER, "models/m3e-base")
    },
    {
        "name": "text-embedding-ada-002",
        "displayName": "text-embedding-ada-002 (openai)",
        "dimension": 1536,
        "enabled": False,
        "link": "https://openai.com"
    }
]


def get_dimension_by_embedding_model(embedding_model):
    for item in SUPPORTED_EMBEDDING_MODELS:
        if item['name'] == embedding_model:
            return item["dimension"]

    raise Exception(f"不支持的 embedding 模型：{embedding_model}")


def get_model_path_by_embedding_model(embedding_model):
    for item in SUPPORTED_EMBEDDING_MODELS:
        if item['name'] == embedding_model:
            return item["model_path"]

    raise Exception(f"不支持的 embedding 模型：{embedding_model}")


def replace_space_n_tab(text):
    """
    替换空格和制表符
    :param text:
    :return:
    """
    pattern = re.compile(r'\s+')  # 匹配任意空白字符，等价于 [\t\n\r\f\v]
    text = re.sub(pattern, '', text)
    return text


def delete_url_email(text):
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')  # url
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')  # email
    text = re.sub(url_pattern, '', text)
    text = re.sub(email_pattern, '', text)
    return text


def txt_pre_process(txt, pre_process_rules):
    """
    文本预处理
    :param txt:
    :return:
    """
    if "replace-space-n-tab" in pre_process_rules:
        txt = txt.replace('\n', '').replace('\t', '')
    if "delete-url-and-email" in pre_process_rules:
        txt = delete_url_email(txt)
    return txt


def chunk_list(input_list, chunk_size):
    """
    Chunk a list into smaller parts of specified size.

    Parameters:
    - input_list: The input list to be chunked.
    - chunk_size: The size of each chunk.

    Returns:
    A list of chunks.
    """
    return [
        input_list[i: i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]


def ensure_directory_exists(dir_path):
    """
        如果目录不存在则创建目录
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def generate_mongoid():
    return str(ObjectId())
