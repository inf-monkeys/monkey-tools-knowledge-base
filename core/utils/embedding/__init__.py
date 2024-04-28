from FlagEmbedding import FlagModel
from loguru import logger
import torch
import os
from core.config import MODELS_FOLDER, embeddings_config
import requests

MODEL_MAP = {}


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
        "type": item.get("type", "local"),
        "apiConfig": item.get("apiConfig"),
    }
    for item in embeddings_config.get("models", [])
]


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


def replace_vars(template, values):
    """
    Recursively replace placeholders in the template with values from the values dictionary.
    This version handles nested structures including dictionaries and lists.

    :param template: dict, a dictionary potentially containing nested dictionaries and lists with placeholders.
    :param values: dict, a dictionary containing values to replace the placeholders.
    :return: dict, a dictionary with the placeholders replaced by actual values, respecting nested structures.
    """
    if isinstance(template, dict):
        result = {}
        for key, value in template.items():
            result[key] = replace_vars(
                value, values
            )  # Recurse into nested dictionaries or lists
    elif isinstance(template, list):
        result = [
            replace_vars(item, values) for item in template
        ]  # Recurse into each item if it's a list
    elif (
        isinstance(template, str)
        and template.startswith("{")
        and template.endswith("}")
    ):
        var_name = template.strip("{}")
        if var_name in values:
            return values[var_name]
        else:
            raise ValueError(
                f"No value provided for variable '{var_name}' in template."
            )
    else:
        return template  # Return the value directly if it's not a placeholder or a complex type

    return result


def get_value_by_path(obj, path):
    if not path:
        return obj
    keys = path.split(">")  # 将路径字符串按 '>' 分割成键的列表
    current_value = obj
    for key in keys:
        if isinstance(current_value, dict) and key in current_value:
            current_value = current_value[key]
        else:
            return None  # 如果路径不可达，则返回 None
    return current_value


def generate_embedding_of_api_model(model_config, documents):
    api_config = model_config.get("apiConfig")
    if not api_config:
        raise Exception(f"缺少 apiConfig 配置：{model_config['name']}")
    api_url = api_config.get("url")
    method = api_config.get("method", "POST")
    if method != "POST":
        raise Exception(f"不支持的请求方法：{method}")

    response_resolver = api_config.get("responseResolver", {})
    type = response_resolver.get("type", "json")
    if type != "json":
        raise Exception(f"不支持的响应解析类型：{type}")
    path = response_resolver.get("path")
    headers = api_config.get("headers", {})
    body = api_config.get("body")
    if body:
        body = replace_vars(body, {"documents": documents})
    logger.info(
        f"请求 API 模型：{api_url}, method: {method}, body: {body}, headers: {headers}"
    )
    r = requests.request(method, api_url, headers=headers, json=body)
    r.raise_for_status()
    json = r.json()
    result = get_value_by_path(json, path)
    if result is None:
        raise Exception(f"无法获取响应结果：{json}")

    # check if the result is a list of lists
    if isinstance(result, list) and all(isinstance(i, list) for i in result):
        return result
    else:
        raise Exception(f"响应结果不是合法的 embeddings result：{result}")


def generate_embedding_of_model(model_name, documents):
    # get model config from SUPPORTED_EMBEDDING_MODELS
    model_config = next(
        (item for item in SUPPORTED_EMBEDDING_MODELS if item["name"] == model_name),
        None,
    )
    if not model_config:
        raise Exception(f"不支持的 embedding 模型：{model_name}")

    if model_config["type"] == "local":
        model = load_model(model_name)
        embeddings = model.encode(documents)
        torch.cuda.empty_cache()
        return embeddings
    elif model_config["type"] == "api":
        return generate_embedding_of_api_model(model_config, documents)
    else:
        raise Exception(f"不支持的 embedding 模型类型：{model_config['type']}")


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
