from job_manager.bm.blobs.base_blob import BaseBlob
from job_manager.ftptools import FtpConnector


class FtpBlob(BaseBlob):
    def __init__(self, host, username, password, root_path):
        self.host = host
        self.username = username
        self.password = password
        self.root_path = root_path

    def create_ftp(self):
        ftp_ctx = FtpConnector()
        ftp_ctx.login(self.host, self.username, self.password, self.root_path)
        ftp_ctx.cwd(self.root_path)
        return ftp_ctx

    def store(self, path, blob_bytes):
        ftp_ctx = self.create_ftp()
        return ftp_ctx.store(path, blob_bytes)

    def load(self, path):
        ftp_ctx = self.create_ftp()
        return ftp_ctx.load(path)
