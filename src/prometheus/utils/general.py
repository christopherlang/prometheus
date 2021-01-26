import datetime
import time
import pytz
import yaml


_YAML_LOADER = yaml.SafeLoader
_YAML_DUMPER = yaml.SafeDumper


class Config:

    def __init__(self, filename, encoding='utf-8'):

        with open(filename, 'r', encoding=encoding) as f:
            self._config_dict = yaml.load(f, Loader=_YAML_LOADER)

        self._filename = filename
        self._encoding = 'utf-8'

    @property
    def raw(self):
        return self._config_dict

    @property
    def filename(self):
        return self._filename

    @property
    def encoding(self):
        return self._encoding

    def general(self, key):
        return self.raw['general'][key]

    def keys(self, key):
        return self.raw['keys'][key]

    def save(self, filename=None):
        if filename is None:
            filename = self._filename

        with open(filename, 'w', encoding=self._encoding) as f:
            yaml.dump(self.raw, f, Dumper=_YAML_DUMPER)


def yesterday_utc():
    return yesterday(tz='UTC')


def yesterday(tz='UTC'):
    """datetime.datetime: Yesterday's datetime. Time is 24 hours prior"""
    today_date = datetime.datetime.utcnow()
    today_date = pytz.UTC.localize(today_date)
    yesterday_date = today_date - datetime.timedelta(days=1)

    return yesterday_date.astimezone(pytz.timezone(tz))


def today(tz='UTC'):
    datetime_now = datetime.datetime.utcfromtimestamp(time.time())
    datetime_now = pytz.UTC.localize(datetime_now)

    if tz.lower() != 'utc':
        result = datetime_now.astimezone(pytz.timezone(tz))
    else:
        result = datetime_now

    return result


def today_utc():
    return today(tz='UTC')


def time_now(tz='UTC'):
    return today(tz=tz).time()


def time_now_utc():
    return time_now(tz='UTC')
