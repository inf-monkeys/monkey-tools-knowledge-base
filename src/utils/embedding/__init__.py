from FlagEmbedding import FlagModel
import torch
import os
from src.utils import ROOT_FOLDER


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
