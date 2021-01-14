import json


# utility class to map idds constants to strings
class IddsConst(object):

    def __init__(self, name):
        self.name = name

    def __getattr__(self, item):
        if self.name:
            return IddsConst('.'.join([self.name, item]))
        return IddsConst(item)

    def __str__(self):
        return self.name


# entry for mapping
constants = IddsConst('')


# json encoder for idds enum constants
class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, IddsConst):
            return {"__idds_const__": str(obj)}
        return json.JSONEncoder.default(self, obj)

