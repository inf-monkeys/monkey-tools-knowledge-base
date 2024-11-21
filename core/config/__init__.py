import os
import yaml

from core.utils import get_host_from_url


def load_config(filename):
    with open(filename, "r") as file:
        config = yaml.safe_load(file)
    return config


config_data = load_config("config.yaml")
database_config = config_data.get("database", {})
vector_config = config_data.get("vector", {})
sql_store_config = config_data.get("sql_store", {})
embeddings_config = config_data.get("embeddings", {"models": []})
proxy_config = config_data.get("proxy", {})
internal_minio_endpoint = config_data.get("internal_minio_endpoint", None)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DOWNLOAD_FOLDER = os.path.join(ROOT_DIR, "download")
SQLITE_FILE_FOLDER = os.path.join(ROOT_DIR, "sqlite-db")
MODELS_FOLDER = os.path.join(ROOT_DIR, "models")

if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

if not os.path.exists(SQLITE_FILE_FOLDER):
    os.makedirs(SQLITE_FILE_FOLDER)

DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_OVERLAP = 50
DEFAULT_SEPARATOR = "\n\n"

if proxy_config.get("enabled", False):
    proxy_url = proxy_config.get("url")
    if not proxy_url:
        raise ValueError("Proxy URL is not provided")
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url

    exclude = proxy_config.get("exclude", [])
    if exclude:
        if not isinstance(exclude, list):
            raise ValueError("Exclude should be a list of strings")

    # Add localhost
    exclude.append("localhost")
    exclude.append("127.0.0.1")

    # Add elasticsearch to the exclude list
    vector_type = vector_config.get("type")
    if vector_type == "elasticsearch":
        es_config = vector_config.get("elasticsearch")
        exclude.append(get_host_from_url(es_config.get("url")))
    os.environ["no_proxy"] = ",".join(exclude)
    os.environ["NO_PROXY"] = ",".join(exclude)

    # Add url models
    for model in embeddings_config.get("models", []):
        if model.get("type") == "api":
            exclude.append(get_host_from_url(model.get("url")))
