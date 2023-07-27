from job_manager.bm.blobs.base_blob import BaseBlob
from job_manager.ftptools import FtpConnector


class FtpBlob(BaseBlob):
    def __init__(self, host, username, password, root_path):
        FtpConnector.instance().login(host, username, password)
        FtpConnector.instance().cwd(root_path)

    def store(self, path, blob_bytes):
        FtpConnector.instance().check_and_connect()
        return FtpConnector.instance().store(path, blob_bytes)

    def load(self, path):
        FtpConnector.instance().check_and_connect()
        return FtpConnector.instance().load(path)
