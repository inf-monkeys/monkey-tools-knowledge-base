import os
import yaml
import os


def load_config(filename):
    with open(filename, "r") as file:
        config = yaml.safe_load(file)
    return config


config_data = load_config("config.yaml")
database_config = config_data.get("database", {})
vector_config = config_data.get("vector", {})
sql_store_config = config_data.get("sql_store", {})
embeddings_config = config_data.get("embeddings", {"models": []})

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
