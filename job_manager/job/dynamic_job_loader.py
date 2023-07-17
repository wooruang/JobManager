import logging

from job_manager import dicttools, stringtools, moduletools
from job_manager.job.job_context import JobContext
from job_manager.job.base_job import BaseJob

LOGGER = logging.getLogger(__name__)


class DynamicJobLoader:
    BASE_JOB_TYPE = BaseJob

    def load_job_list(self):
        parent_module = moduletools.parent_module(self.BASE_JOB_TYPE)

        inner_module_list = moduletools.find_inner_modules(parent_module)

        class_list = moduletools.find_class_in_modules(inner_module_list)

        filtered_class_list = moduletools.find_child_class(class_list, self.BASE_JOB_TYPE)

        filtered_class_list = self.remove_category_job(filtered_class_list)

        descriptions = self._make_hierarchy_descriptions_of_class_list(filtered_class_list)

        return descriptions

    @staticmethod
    def remove_category_job(class_list):
        CATEGORY_JOB_NAME = 'CategoryJob'
        new_class_list = []
        for c in class_list:
            if CATEGORY_JOB_NAME in c.__name__:
                continue
            new_class_list.append(c)
        return new_class_list

    @staticmethod
    def _make_hierarchy_descriptions_of_class_list(class_list):
        class_dict = {}
        for c in class_list:
            parents = c.__module__.split('.')[:-1]
            base_dict = {}
            temp_dict = base_dict
            for p in parents:
                temp_dict[p] = {}
                temp_dict = temp_dict[p]

            if not hasattr(c, 'name'):
                continue

            temp_dict[c.__name__] = {
                'name': c.name,
                'description': c.description,
                'parameters': c.parameters_to_str()
            }
            dicttools.dict_merge(class_dict, base_dict)
        return class_dict

    def load_job(self, job_id):
        descriptions = self.load_job_list()
        desc = descriptions
        try:
            for j_id in job_id:
                desc = desc[j_id]
        except Exception as e:
            return None
        return desc

    def create(self, data):
        job = None
        try:
            job_instance = self._load_job_module(data['category'], data['name'])
            context = data['context'] if 'context' in data else None
            from_job = data['from_job'] if 'from_job' in data else None
            job = job_instance.create(context, data['condition'], from_job)
        except Exception as e:
            LOGGER.error('Error %s', e, exc_info=True)
        return job

    def generate(self, job: JobContext):
        job_data = None
        try:
            job_instance = self._load_job_module(job.category, job.name)
            job_data = job_instance.generate(job)
        except Exception as e:
            LOGGER.error('Error %s', e, exc_info=True)
        return job_data

    def execute(self, data):
        try:
            job_instance = self._load_job_module(data['category'], data['name'])
            job_instance.execute(data)
        except Exception as e:
            LOGGER.error('Error %s', e, exc_info=True)

    def stage_info(self, category, name):
        try:
            job_instance = self._load_job_module(category, name)
        except Exception as e:
            LOGGER.error('Error %s', e, exc_info=True)
            return None
        return job_instance.stages()

    def _load_job_module(self, category, name):
        job_module = moduletools.import_module(
            f'{".".join(self.BASE_JOB_TYPE.PARENT_PACKAGE)}.{category}.{stringtools.camel_to_snake(name)}', name)
        return job_module()
