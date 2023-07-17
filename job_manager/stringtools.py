import re

CAMEL_TO_SNAKE_PATTERN = re.compile(r'(?<!^)(?=[A-Z])')


def camel_to_snake(src_str):
    return CAMEL_TO_SNAKE_PATTERN.sub('_', src_str).lower()
