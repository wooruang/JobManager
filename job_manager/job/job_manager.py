class JobManager:

    def job_list(self):
        job_list = []
        json_static_job_list = self._load_json_static_job_list()
        job_list.append(json_static_job_list)

        dynamic_job_list = self._load_dynamic_job_list()
        job_list.append(dynamic_job_list)

        return job_list

    def _load_json_static_job_list(self):
        pass

    def _load_dynamic_job_list(self):
        pass
