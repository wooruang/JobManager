import uuid

from job_manager.bm.blobs.ftp_blob import FtpBlob
from job_manager.singleton import SingletonInstance


class BlobManager(SingletonInstance):

    def __init__(self):
        self.ftp_blob = None

    def init_ftp(self, host, username, password, root_path):
        self.ftp_blob = FtpBlob(host, username, password, root_path)

    def store(self, store_location, job_id, job_task_id, blob_ext, blob_bytes, filename=None):
        """
        Store bytes.

        Parameters
        ----------
        store_location :  str
            저장 위치 [ 'ftp' , ]
        job_id
            Job ID
        job_task_id
            Job Task id
        blob_ext
            Blob 데이터 확장자
        blob_bytes
            데이터
        filename : str
            파일 이름
        Returns
        -------

        """
        if filename is None:
            filename = f'{str(uuid.uuid4())}.{blob_ext}'

        path = f'{job_id}/{job_task_id}/{filename}'
        self.store_by_path(store_location, path, blob_bytes)

    def store_by_path(self, store_location, path, blob_bytes, retry=1):
        """
        Store bytes.

        Parameters
        ----------
        store_location :  str
            저장 위치 [ 'ftp' , ]
        path
        blob_bytes
            데이터
        retry

        Returns
        -------

        """
        if store_location == 'ftp' and self.ftp_blob is not None:
            return self._store_ftp(path, blob_bytes, retry)
        elif self.ftp_blob is None:
            raise ValueError('Not initialized ftp!')
        else:
            raise ValueError(f'Not Supported!', store_location)

    def _store_ftp(self, path, blob_bytes, retry=1):
        ret = self.ftp_blob.store(path, blob_bytes)
        while ret is None and retry > 0:
            ret = self.ftp_blob.store(path, blob_bytes)
            retry -= 1
        return ret

    def load_by_path(self, store_location, path, retry=1):
        """

        Parameters
        ----------
        store_location
        path
        retry

        Returns
        -------

        """
        if store_location == 'ftp' and self.ftp_blob is not None:
            return self._load_ftp(path, retry)
        elif self.ftp_blob is None:
            raise ValueError('Not initialized ftp!')
        else:
            raise ValueError(f'Not Supported!', store_location)

    def _load_ftp(self, path, retry=1):
        ret = self.ftp_blob.load(path)
        while ret is None and retry > 0:
            ret = self.ftp_blob.load(path)
            retry -= 1
        return ret
