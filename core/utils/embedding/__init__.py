from FlagEmbedding import FlagModel
import torch
import os
from core.config import MODELS_FOLDER, embeddings_config

MODEL_MAP = {}


def load_model(model_name):
    global MODEL_MAP
    if MODEL_MAP.get(model_name):
        return MODEL_MAP.get(model_name)
    model_path = get_model_path_by_embedding_model(model_name)
    model = FlagModel(
        # 如果本地有下载 model 使用本地的，否则在线下载
        model_path if os.path.exists(model_path) else model_name,
        use_fp16=True,
    )  # Setting use_fp16 to True speeds up computation with a slight performance degradation
    MODEL_MAP[model_name] = model
    return model


def generate_embedding_of_model(model_name, q):
    model = load_model(model_name)
    embeddings = model.encode(q)
    torch.cuda.empty_cache()
    return embeddings

def remove_model_name_prefix(model_name):
    return model_name.split("/")[-1]

SUPPORTED_EMBEDDING_MODELS = [
    {
        "name": item["name"],
        "displayName": item.get("displayName", item["name"]),
        "dimension": item["dimension"],
        "enabled": item.get("enabled", True),
        "model_path": item.get("modelPath")
        or os.path.join(MODELS_FOLDER, remove_model_name_prefix(item["name"])),
    }
    for item in embeddings_config.get("models", [])
]

def get_dimension_by_embedding_model(embedding_model):
    for item in SUPPORTED_EMBEDDING_MODELS:
        if item["name"] == embedding_model:
            return item["dimension"]

    raise Exception(f"不支持的 embedding 模型：{embedding_model}")


def get_model_path_by_embedding_model(embedding_model):
    for item in SUPPORTED_EMBEDDING_MODELS:
        if item["name"] == embedding_model:
            return item["model_path"]

    raise Exception(f"不支持的 embedding 模型：{embedding_model}")
