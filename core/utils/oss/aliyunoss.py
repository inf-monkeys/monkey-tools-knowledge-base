import oss2
import re
import datetime


class AliyunOSSClient:
    def __init__(self, endpoint, bucket_name, access_key, secret_key):
        self.bucket_name = bucket_name
        auth = oss2.Auth(access_key, secret_key)
        self.bucket = oss2.Bucket(auth, endpoint, bucket_name)

    def test_connection(self, prefix, max_take=1):
        files = []
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix):
            files.append(obj.key)
            if len(files) >= max_take:
                break
        return files

    def read_dir(self, prefix):
        """
        列举对象
        :param prefix: 路径
        :return:
        """
        files = []
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix):
            files.append(obj.key)
        return files

    def get_signed_url(self, object_name, expires=3600):
        url = self.bucket.sign_url('GET', object_name, expires)
        return url

    def is_file_match_condition(self, file, fileExtensions, excludeFileRegex):
        # 如果后缀不在合法的后缀中，不符合
        if fileExtensions:
            match_extension_result = [
                file.endswith(extension) for extension in fileExtensions
            ]
            if True not in match_extension_result:
                return False

        if excludeFileRegex:
            if re.compile("r'{}'".format(excludeFileRegex.replace("\\", "\\\\"))).search(file):
                return False

        return True

    def get_all_files_in_base_folder(self, base_folder, fileExtensions=None, excludeFileRegex=None):
        files = self.read_dir(base_folder)
        valid_files = []
        for file in files:
            if self.is_file_match_condition(file, fileExtensions, excludeFileRegex):
                valid_files.append(file)
        return valid_files
