import os
from pathlib import Path
import uuid
import zipfile

from loguru import logger
from core.utils.oss import download_file


def extract_files_from_zip(zip_url):
    zip_file = download_file(zip_url)
    extract_to = os.path.join(os.path.dirname(zip_file), str(uuid.uuid4()))
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    txt_files = Path(extract_to).rglob("**/*.txt")
    md_files = Path(extract_to).rglob("**/*.md")
    pdf_files = Path(extract_to).rglob("**/*.pdf")
    json_files = Path(extract_to).rglob("**/*.json")
    jsonl_files = Path(extract_to).rglob("**/*.jsonl")
    csv_files = Path(extract_to).rglob("**/*.csv")
    docx_files = Path(extract_to).rglob("**/*.docx")
    xlsx_files = Path(extract_to).rglob("**/*.xlsx")
    eml_files = Path(extract_to).rglob("**/*.eml")
    all_files = (
        list(txt_files)
        + list(md_files)
        + list(pdf_files)
        + list(json_files)
        + list(jsonl_files)
        + list(csv_files)
        + list(docx_files)
        + list(xlsx_files)
        + list(eml_files)
    )
    valid_files = []
    for file in all_files:
        if "__MACOSX" not in str(file):
            valid_files.append(str(file))
    logger.info(f"Load {len(valid_files)} files from {zip_file}")
    os.remove(zip_file)
    return extract_to, valid_files
