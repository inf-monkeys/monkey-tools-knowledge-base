from langchain.document_loaders import TextLoader, PyMuPDFLoader, CSVLoader, UnstructuredFileLoader, \
    UnstructuredMarkdownLoader, \
    JSONLoader, UnstructuredWordDocumentLoader, UnstructuredPowerPointLoader, UnstructuredEmailLoader
from .loaders.image_loader import UnstructuredPaddleImageLoader
from .splitters.chinese_text_splitter import ChineseTextSplitter
import zipfile
import os
import uuid
from pathlib import Path
import shutil
import pandas as pd
from .processors import document_process


def load_single_document(file_path, jq_schema=None, pre_process_rules=[]):
    if file_path.lower().endswith('.pdf'):
        loader = PyMuPDFLoader(file_path=file_path)
    elif file_path.lower().endswith('.csv'):
        loader = CSVLoader(file_path=file_path)
    elif file_path.lower().endswith('.xlsx'):
        csv_file_path = file_path[:-5] + '.csv'
        xlsx = pd.read_excel(file_path, engine='openpyxl')
        xlsx.to_csv(csv_file_path, index=False)
        loader = CSVLoader(csv_file_path, csv_args={"delimiter": ",", "quotechar": '"'})
    elif file_path.lower().endswith('.txt'):
        loader = TextLoader(file_path=file_path)
    elif file_path.lower().endswith('.md'):
        loader = UnstructuredMarkdownLoader(file_path=file_path)
    elif file_path.lower().endswith('.json'):
        loader = JSONLoader(file_path=file_path, jq_schema=jq_schema, text_content=False)
    elif file_path.lower().endswith('.jsonl'):
        loader = JSONLoader(file_path=file_path, json_lines=True, jq_schema=jq_schema, text_content=False)
    elif file_path.lower().endswith('.docx'):
        loader = UnstructuredWordDocumentLoader(file_path, mode="elements")
    elif file_path.lower().endswith(".pptx"):
        loader = UnstructuredPowerPointLoader(file_path, mode="elements")
    elif file_path.lower().endswith(".eml"):
        loader = UnstructuredEmailLoader(file_path, mode="elements")
    else:
        loader = UnstructuredFileLoader(file_path=file_path)

    documents = loader.load()

    if len(pre_process_rules) > 0:
        documents = document_process(documents, pre_process_rules)
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
        csv_files = Path(extract_to).rglob('**/*.csv')
        docx_files = Path(extract_to).rglob('**/*.docx')
        xlsx_files = Path(extract_to).rglob('**/*.xlsx')
        eml_files = Path(extract_to).rglob('**/*.eml')
        all_files = list(txt_files) + list(md_files) + list(pdf_files) + list(json_files) + list(jsonl_files) + list(
            csv_files) + list(docx_files) + list(xlsx_files) + list(eml_files)
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
        text_splitter = ChineseTextSplitter(
            separator=separator,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            is_separator_regex=True,
            sentence_size=chunk_size
        )
        texts = text_splitter.split_documents(documents)
        print(f"使用 CharacterTextSplitter 切割到 {len(texts)} 个片段")
        return texts
