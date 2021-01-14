from . import Client


# API call class
class IddsApi(object):

    def __init__(self, name):
        self.name = name

    def __call__(self, *args, **kwargs):
        return Client.call_idds_command(self.name, args, kwargs)


# interface to API
class IddsApiInteface(object):
    def __init__(self):
        pass

    def __getattr__(self, item):
        return IddsApi(item)


# entry for API
api = IddsApiInteface()
del IddsApiInteface


def get_api():
    return api