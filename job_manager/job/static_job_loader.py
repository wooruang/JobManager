import json
import os

from job_manager import filetools


class StaticJobLoader:
    RESOURCES_ROOT: str
    EXTENSIONS: list

    def load_job_list(self):
        json_paths = filetools.get_paths_by_extensions(self.RESOURCES_ROOT, self.EXTENSIONS)
        json_rel_path = [os.path.relpath(p, os.path.abspath(self.RESOURCES_ROOT)) for p in json_paths]
        json_list = [filetools.remove_extension(p) for p in json_rel_path]
        return json_list

    def request(self, path):
        data = None
        for ext in self.EXTENSIONS:
            json_path = os.path.join(self.RESOURCES_ROOT, f'{path}.{ext}')
            if os.path.exists(json_path):
                data = self._load_file(json_path)
                data = json.loads(data)
                break
        return data

    @staticmethod
    def _load_file(path):
        with open(path, 'r') as f:
            return f.read()
