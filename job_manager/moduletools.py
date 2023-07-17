import importlib
import pkgutil
import inspect


def parent_module_name_by_str(module_name):
    return '.'.join(module_name.split('.')[:-1])


def parent_module_by_str(module_name):
    return importlib.import_module(parent_module_name_by_str(module_name))


def parent_module_name(module):
    return parent_module_name_by_str(module.__module__)


def parent_module(module):
    return parent_module_by_str(module.__module__)


def find_inner_modules(module):
    module_list = []
    for importer, modname, _ in pkgutil.walk_packages(module.__path__):
        m = importer.find_module(modname).load_module(modname)
        module_list.append(m)
    return module_list


def find_class_in_modules(modules):
    class_list = []
    for module in modules:
        module_dict = module.__dict__
        for k in module_dict:
            if inspect.isclass(module_dict[k]):
                class_list.append(module_dict[k])
    return class_list


def parent_classes(module, parents=None):
    if module.__name__ == 'object':
        return parents

    if parents is None:
        parents = []

    for base in module.__bases__:
        if base.__name__ == 'object':
            continue
        parents.append(base)
        parent_classes(base, parents)

    return parents


def find_child_class(class_list, parent):
    filtered_class_list = []
    for c in class_list:
        if c.__name__ == parent.__name__:
            continue

        parent_name_list = [x.__name__ for x in parent_classes(parent)]

        if c.__name__ in parent_name_list:
            continue

        for c_p in parent_classes(c):
            if c_p.__name__ in parent_name_list:
                break
        else:
            continue

        filtered_class_list.append(c)

    return filtered_class_list


def import_module_by_list(target_module):
    return getattr(importlib.import_module('.'.join(target_module[:-1])), target_module[-1])


def import_module(category, name):
    return getattr(importlib.import_module(category), name)
