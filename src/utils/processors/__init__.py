import re
from .ZhTitleEnhance import is_possible_title


def replace_space_n_tab(text):
    """
    替换空格和制表符
    :param text:
    :return:
    """
    pattern = re.compile(r'\s+')  # 匹配任意空白字符，等价于 [\t\n\r\f\v]
    text = re.sub(pattern, '', text)
    return text


def delete_url_email(text):
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')  # url
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')  # email
    text = re.sub(url_pattern, '', text)
    text = re.sub(email_pattern, '', text)
    return text


def txt_pre_process(text, pre_process_rules):
    if "replace-space-n-tab" in pre_process_rules:
        text = replace_space_n_tab(text)
    if "delete-url-and-email" in pre_process_rules:
        text = delete_url_email(text)
    return text


def document_process(documents, pre_process_rules):
    title = None
    for doc in documents:
        if "replace-space-n-tab" in pre_process_rules:
            doc.page_content = replace_space_n_tab(doc.page_content)
        if 'delete-url-and-email' in pre_process_rules:
            doc.page_content = delete_url_email(doc.page_content)
        if 'zh_title_enhance':
            if is_possible_title(doc.page_content):
                doc.metadata['category'] = 'cn_Title'
                title = doc.page_content
            elif title:
                doc.page_content = doc.page_content = f"下文与({title})有关。{doc.page_content}"
    return documents
