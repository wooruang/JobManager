import os
import io

from job_manager.bm.blobs.base_blob import BaseBlob


class FileSystemBlob(BaseBlob):
    def __init__(self, root_path):
        self.root_path = root_path

    def store(self, path, blob_bytes):
        dest_path = os.path.join(self.root_path, path)
        parent_path = os.path.dirname(dest_path)
        os.makedirs(parent_path, exist_ok=True)
        with open(dest_path, 'wb') as f:
            f.write(blob_bytes.read())
        return dest_path

    def load(self, path):
        dest_path = os.path.join(self.root_path, path)
        data_bytes = None
        with open(dest_path, 'wb') as f:
            data_bytes = io.BytesIO(f.read())
        return data_bytes
