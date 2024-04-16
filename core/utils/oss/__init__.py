import os
from urllib.parse import unquote
import requests
from loguru import logger
from core.config import DOWNLOAD_FOLDER


def extract_filename(url):
    filename = url.split("/")[-1]
    filename = filename.split("?")[0]
    return unquote(filename)


def download_file(file_url):
    """
    下载文件进指定目录
    下载成功返回 文件地址
    下载失败返回 False
    """
    try:
        response = requests.get(file_url, stream=True)
        response.raise_for_status()
        filename = extract_filename(file_url)
        final_path = os.path.join(DOWNLOAD_FOLDER, filename)
        with open(final_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return final_path
    except requests.RequestException as e:
        logger.error(f"下载文件失败，错误信息为 {e}")
        return False
