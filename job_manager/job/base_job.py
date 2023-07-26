from typing import List


class BaseJob:
    PARENT_PACKAGE: list
    name: str
    description: str
    all_stage: List[str]
    job_type: str = 'DefaultJob'

    def create(self, context, condition, from_job=None):
        pass

    def generate(self, job):
        pass

    def request(self, **kwargs):
        pass

    def names_of_all_stages(self):
        stages = self.stages()
        if stages is None:
            return self.all_stage
        names = [x['name'] for x in stages]
        return names

    @staticmethod
    def parameters():
        pass

    @staticmethod
    def stages():
        pass

    @classmethod
    def parameters_to_str(cls):
        params = cls.parameters()
        for k in params:
            tokens = str(params[k]['type']).split('[')
            if len(tokens) == 1:
                type_name = cls.remove_parent(params[k]['type'].__name__)
            elif len(tokens) == 2:
                main_type = cls.remove_parent(tokens[0])
                sub_type = cls.remove_parent(tokens[1][:-1])
                type_name = f'{main_type}[{sub_type}]'
            else:
                raise ValueError("A parameter's type of job is wrong value", params[k]['type'])
            params[k]['type'] = type_name
        return params

    @classmethod
    def remove_parent(cls, type_name):
        tokens = type_name.split('.')
        if tokens[0] == 'typing':
            return '.'.join(tokens[1:])
        elif len(tokens) > len(cls.PARENT_PACKAGE):
            for idx, p in enumerate(cls.PARENT_PACKAGE):
                if tokens[idx] != p:
                    break
            else:
                return '.'.join(tokens[len(cls.PARENT_PACKAGE):])
        return type_name
