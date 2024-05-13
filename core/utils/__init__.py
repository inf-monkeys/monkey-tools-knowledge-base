from urllib.parse import urlparse
import uuid
import os
from random import choice
from string import ascii_letters
from shortid import ShortId
import hashlib

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

def get_host_from_url(url: str) -> str:
    try:
        parsed_url = urlparse(url)
        return f"{parsed_url.hostname}:{parsed_url.port}" if parsed_url.port else parsed_url.hostname
    except Exception as e:
        print(f"Invalid URL: {e}")
        return ''
