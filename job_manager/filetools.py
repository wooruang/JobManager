import os


def get_paths_by_extensions(root_dir, match_exts):
    match_files = []
    for root, dirnames, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.split('.')[-1] in match_exts:
                p = os.path.join(root, fn)
                match_files.append(p)
    return match_files


def basename_without_extension(p):
    b = os.path.basename(p).split('.')
    if len(b) == 1:
        return b[0]
    return '.'.join(b[:-1])


def remove_extension(p):
    return '.'.join(p.split('.')[:-1])
