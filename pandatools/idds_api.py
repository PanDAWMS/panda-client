from . import Client


# API call class
class IddsApi(object):

    def __init__(self, name, dumper, verbose):
        self.name = name
        self.dumper = dumper
        self.verbose = verbose

    def __call__(self, *args, **kwargs):
        return Client.call_idds_command(self.name, args, kwargs, self.dumper, self.verbose)


# interface to API
class IddsApiInteface(object):
    def __init__(self):
        self.dumper = None

    def __getattr__(self, item):
        return IddsApi(item, self.dumper, self.verbose)

    def setup(self, dumper, verbose):
        self.dumper = dumper
        self.verbose = verbose


# entry for API
api = IddsApiInteface()
del IddsApiInteface


def get_api(dumper=None, verbose=False):
    """Get an API object to access iDDS through PanDA

       args:
           dumper: function object to json-serialize data
           verbose: True to see verbose messages
       return:
           an API object
    """
    api.setup(dumper, verbose)
    return api
