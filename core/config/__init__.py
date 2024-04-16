import os
import yaml


def load_config(filename):
    with open(filename, 'r') as file:
        config = yaml.safe_load(file)
    return config


config_data = load_config('config.yaml')
database_config = config_data.get('database', {})
vector_config = config_data.get('vector', {})


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DOWNLOAD_FOLDER = os.path.join(ROOT_DIR, "download")

DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_OVERLAP = 50
DEFAULT_SEPARATOR = "\n\n"
