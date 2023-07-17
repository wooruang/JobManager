import importlib
import pkgutil
import inspect
import base64
import typing

SEARCH_TASK_MODULE_NAME = 'common.task.tasks'

#
# receive
# front -> manager -> loader -> job list

# front -> manager -> json static -> job list
# front -> manager -> db static -> job list

# save
# front -> manager -> job -> registry schedule job
# front -> manager -> job -> execute
# scheduler -> manager -> job -> execute

# class JobLoader:


# Load Task's list.
def load_task_list():
    tasks_module = importlib.import_module(SEARCH_TASK_MODULE_NAME)

    module_list = find_inner_modules(tasks_module)
    all_task_modules = find_registered_task_modules(module_list)

    return all_task_modules


def load_task_infos_as_json():
    task_infos = load_task_list()
    task_infos_as_json = [x().to_json() for x in task_infos]
    return task_infos_as_json


def load_task_infos_as_dict():
    task_infos = load_task_list()
    task_infos_as_dict = [x().__dict__ for x in task_infos]
    return task_infos_as_dict


def find_inner_modules(module):
    module_list = []
    for importer, modname, _ in pkgutil.walk_packages(module.__path__):
        m = importer.find_module(modname).load_module(modname)
        module_list.append(m)
    return module_list


def find_registered_task_modules(module_list):
    all_task_modules = []
    for module in module_list:
        task_modules = filter_modules_for_registered_task(module)
        if not task_modules:
            continue
        all_task_modules.extend(task_modules)
    return all_task_modules


def filter_modules_for_registered_task(module):
    if not hasattr(module, '__dict__'):
        raise ValueError(f"It it not a module! ({module.__name__})")

    module_dict = module.__dict__

    result = []
    for k in module_dict:
        m = module_dict[k]
        if not check_registered_task(m):
            continue
        result.append(m)
    return result


def check_registered_task(module):
    if not hasattr(module, '__dict__'):
        return False
    if not hasattr(module, 'task_name'):
        return False
    return True


# Get a task info()
def get_task_info(task_id):
    task_module_path = base64.urlsafe_b64decode(task_id).decode()
    task_module_tokens = task_module_path.split('.')

    if len(task_module_tokens) > 1:
        parent_task_module_path = '.'.join(task_module_tokens[:-1])
        parent_task_module_name = f"{SEARCH_TASK_MODULE_NAME}.{parent_task_module_path}"
        task_module_name = task_module_tokens[-1]
    else:
        parent_task_module_name = f"{SEARCH_TASK_MODULE_NAME}"
        task_module_name = task_module_tokens[0]

    parent_tasks_module = importlib.import_module(parent_task_module_name)

    tasks_module = parent_tasks_module.__dict__[task_module_name]

    if not check_registered_task(tasks_module):
        return None

    task_info = tasks_module().__dict__
    # print(tasks_module.__dict__)
    # print(tasks_module().__dict__)
    # print(inspect.getfullargspec(tasks_module.__dict__['run']))
    args_annos = get_function_args_annotation(tasks_module.__dict__['run'])
    task_info['args'] = args_annos
    return task_info


def get_function_args_annotation(func):
    spec = inspect.getfullargspec(func)
    args_annotations = {}
    for k in spec.annotations:
        annos = spec.annotations[k]
        if inspect.isclass(annos):
            args_annotations[k] = annos.__name__
        elif isinstance(annos, typing._GenericAlias):
            if len(annos.__args__) > 1:
                raise TypeError(annos.__args__)
            args_annotations[k] = f'{annos._name}[{annos.__args__[0].__name__}]'
        else:
            raise TypeError(annos)
    return args_annotations


# Decorator for a task.
def task_register(cls):
    """
    Register as a task.

    Parameters
    ----------
    cls

    Returns
    -------
    cls added a method of 'task_name'.

    """
    def get_task_name():
        return f'{cls.__module__}.{cls.__name__}'

    setattr(cls, 'task_name', get_task_name)
    return cls


if __name__ == '__main__':
    print(load_task_list())
    t1 = load_task_list()[0]()
    print(t1.id)
    print(get_task_info(t1.id))
