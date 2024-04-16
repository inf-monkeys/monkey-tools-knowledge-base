import tos
from tos import HttpMethodType
import re


class TOSClient:
    def __init__(self, endpoint, region, bucket_name, access_key, secret_key):
        self.bucket_name = bucket_name
        # 超时时间 单位秒
        self.client = tos.TosClientV2(
            access_key, secret_key, endpoint, region, connection_time=30
        )

    def test_connection(self, tos_path):
        out = self.client.list_objects_type2(
            self.bucket_name, delimiter="/", prefix=tos_path, continuation_token=""
        )
        files_in_dir = [item.key for item in out.contents]
        folders_in_dir = [prefix.prefix for prefix in out.common_prefixes]
        return {"files_in_dir": files_in_dir, "folders_in_dir": folders_in_dir}

    def _read_dir(self, tos_path, continuation_token=""):
        """
        列举对象
        :param tos_path:
        :return:
        """
        files = []
        out = self.client.list_objects_type2(
            self.bucket_name,
            delimiter="/",
            prefix=tos_path,
            continuation_token=continuation_token,
        )
        next_continuation_token = out.next_continuation_token
        # common_prefixes中返回了fun1/目录下的子目录
        files_in_dir = [item.key for item in out.contents]
        files += files_in_dir
        folders_in_dir = [prefix.prefix for prefix in out.common_prefixes]
        for folder in folders_in_dir:
            files += self._read_dir(folder)
        if next_continuation_token:
            files += self._read_dir(tos_path, next_continuation_token)
        return files

    def get_signed_url(self, key, expires=3600):
        return self.client.pre_signed_url(
            HttpMethodType.Http_Method_Get, self.bucket_name, key=key, expires=expires
        ).signed_url

    def _is_file_match_condition(self, file, fileExtensions, excludeFileRegex):
        # 如果后缀不在合法的后缀中，不符合
        if fileExtensions:
            match_extension_result = [
                file.endswith(extension) for extension in fileExtensions
            ]
            if True not in match_extension_result:
                return False

        if excludeFileRegex:
            if re.compile(
                "r'{}'".format(excludeFileRegex.replace("\\", "\\\\"))
            ).search(file):
                return False

        return True

    def read_base_folder(self, base_folder, fileExtensions=None, excludeFileRegex=None):
        files = self._read_dir(base_folder)
        valid_files = []
        for file in files:
            # 如果是目录，跳过
            if file.endswith("/"):
                continue
            if self._is_file_match_condition(file, fileExtensions, excludeFileRegex):
                valid_files.append(file)
        return valid_files
