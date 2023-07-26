import datetime

DEFAULT_FORMAT = '%Y-%m-%dT%H:%M:%S'


def is_time_by_conditions(repeat, period, every, last_time: datetime.datetime, current=None):
    if current is None:
        current = datetime.datetime.now()

    if repeat is not None and repeat > 0:
        return True

    if period is not None and period:
        if last_time is None:
            return True

        delta = current - last_time
        if convert_time_str_to_timedelta(period) > delta:
            return False
        return True
    elif every is not None and every:
        if last_time is None:
            return False

        now_every = convert_partial_datetime_str_by_current(every, current)
        if now_every is None:
            return False
        delta = now_every - last_time

        if delta.total_seconds() > 0:
            return False
        return True
    else:
        return False


def convert_time_str_to_timedelta(time_str):
    if time_str is None:
        return None
    if isinstance(time_str, float) or isinstance(time_str, int):
        return datetime.timedelta(milliseconds=time_str)
    if not isinstance(time_str, str):
        raise ValueError('Wrong format for str_time', time_str)
    if time_str[-1].lower() not in ['w', 'd', 'h', 'm', 's']:
        raise ValueError('Wrong format for str_time', time_str)

    str_time_number = int(time_str[:-1])
    str_time_unit = time_str[-1].lower()

    if str_time_unit == 'w':
        return datetime.timedelta(weeks=7)
    elif str_time_unit == 'd':
        return datetime.timedelta(days=str_time_number)
    elif str_time_unit == 'h':
        return datetime.timedelta(hours=str_time_number)
    elif str_time_unit == 'm':
        return datetime.timedelta(minutes=str_time_number)
    elif str_time_unit == 's':
        return datetime.timedelta(seconds=str_time_number)
    else:
        raise ValueError('Wrong format for str_time', str_time_unit)


def convert_timedelta_str_to_datetime(time_str, now=None):
    dt = convert_time_str_to_timedelta(time_str)
    if now is None:
        now = datetime.datetime.now()
    return now + dt


def convert_partial_datetime_str_by_current(datetime_str: str, current=None):
    if datetime_str is None:
        return None
    if current is None:
        current = datetime.datetime.now()
    if not isinstance(datetime_str, str):
        raise ValueError('Wrong format for every', datetime_str)
    now_str = current.strftime(DEFAULT_FORMAT)
    now_time = now_str[:-len(datetime_str)] + datetime_str
    try:
        datetime_by_now = datetime.datetime.strptime(now_time, DEFAULT_FORMAT)
    except:
        return None
    return datetime_by_now


def str_to_datetime(datetime_str, datetime_format=DEFAULT_FORMAT):
    return datetime.datetime.strptime(datetime_str, datetime_format)


def datetime_to_str(datetime_d: datetime.datetime, datetime_format=DEFAULT_FORMAT):
    return datetime_d.strftime(datetime_format)
