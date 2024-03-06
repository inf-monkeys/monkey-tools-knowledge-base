from langchain.document_loaders import TextLoader, PyMuPDFLoader, CSVLoader, UnstructuredFileLoader, \
    UnstructuredMarkdownLoader, \
    JSONLoader
from langchain.text_splitter import CharacterTextSplitter
import zipfile
import os
import uuid
from pathlib import Path
import shutil

from src.utils import txt_pre_process


def load_single_document(file_path, jq_schema=None, pre_process_rules=[]):
    file_ext = file_path.split('.')[-1]
    if file_ext == 'pdf':
        loader = PyMuPDFLoader(file_path=file_path)
    elif file_ext == 'csv':
        loader = CSVLoader(file_path=file_path)
    elif file_ext == 'txt':
        loader = TextLoader(file_path=file_path)
    elif file_ext == 'md':
        loader = UnstructuredMarkdownLoader(file_path=file_path)
    elif file_ext == '.json':
        loader = JSONLoader(file_path=file_path, jq_schema=jq_schema, text_content=False)
    elif file_ext == '.jsonl':
        loader = JSONLoader(file_path=file_path, json_lines=True, jq_schema=jq_schema, text_content=False)
    else:
        loader = UnstructuredFileLoader(file_path=file_path)
    documents = loader.load()

    if len(pre_process_rules) > 0:
        for doc in documents:
            doc.page_content = txt_pre_process(doc.page_content, pre_process_rules)

    return documents


def load_documents(
        file_path: str,
        chunk_size=2048,
        chunk_overlap=0,
        separator='\n\n',
        pre_process_rules=[],
        jqSchema=None
):
    file_ext = file_path.split('.')[-1]
    documents = []
    if file_ext == 'zip':
        extract_to = os.path.join(os.path.dirname(file_path), str(uuid.uuid4()))
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        txt_files = Path(extract_to).rglob('**/*.txt')
        md_files = Path(extract_to).rglob('**/*.md')
        pdf_files = Path(extract_to).rglob('**/*.pdf')
        json_files = Path(extract_to).rglob('**/*.json')
        jsonl_files = Path(extract_to).rglob('**/*.jsonl')
        all_files = list(txt_files) + list(md_files) + list(pdf_files) + list(json_files) + list(jsonl_files)
        valid_files = []
        for file in all_files:
            if "__MACOSX" not in str(file):
                valid_files.append(str(file))
        print("从 zip 文件中加载到以下文件：", valid_files)
        for file in valid_files:
            documents.extend(load_single_document(file, pre_process_rules=pre_process_rules,
                                                  jq_schema=jqSchema
                                                  ))
        os.remove(file_path)
        shutil.rmtree(extract_to)
    else:
        documents = load_single_document(file_path, pre_process_rules=pre_process_rules, jq_schema=jqSchema)
        os.remove(file_path)

    print(f"使用 Loader 加载到 {len(documents)} 个文本")

    if jqSchema:
        return documents
    else:
        separator = separator.replace('\\n', '\n')
        text_splitter = CharacterTextSplitter(
            separator=separator,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            is_separator_regex=True
        )
        texts = text_splitter.split_documents(documents)
        print(f"使用 CharacterTextSplitter 切割到 {len(texts)} 个片段")
        return texts
