from . import Client


# API call class
class IddsApi(object):

    def __init__(self, name, dumper, verbose, idds_host, compress, manager, loader):
        self.name = name
        if idds_host is not None:
            self.name += '+{}'.format(idds_host)
        self.dumper = dumper
        self.verbose = verbose
        self.compress = compress
        self.manager = manager
        self.loader = loader

    def __call__(self, *args, **kwargs):
        return Client.call_idds_command(self.name, args, kwargs, self.dumper, self.verbose, self.compress,
                                        self.manager, self.loader)


# interface to API
class IddsApiInteface(object):
    def __init__(self):
        self.dumper = None
        self.loader = None

    def __getattr__(self, item):
        return IddsApi(item, self.dumper, self.verbose, self.idds_host, self.compress, self.manager,
                       self.loader)

    def setup(self, dumper, verbose, idds_host, compress, manager, loader):
        self.dumper = dumper
        self.verbose = verbose
        self.idds_host = idds_host
        self.compress = compress
        self.manager = manager
        self.loader = loader


# entry for API
api = IddsApiInteface()
del IddsApiInteface


def get_api(dumper=None, verbose=False, idds_host=None, compress=True, manager=False, loader=None):
    """Get an API object to access iDDS through PanDA

       args:
           dumper: function object to dump json-serialized data
           verbose: True to see verbose messages
           idds_host: iDDS hostname
           compress: True to compress request body
           manager: True to use ClientManager API
           loader: function object to load json-serialized data
       return:
           an API object
    """
    api.setup(dumper, verbose, idds_host, compress, manager, loader)
    return api
