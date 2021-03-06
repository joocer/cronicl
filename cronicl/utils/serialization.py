"""
There is no one JSON library which is best.

orjson is about 2x faster than ujson and 5x json at serialization

ujson usually deserializes faster than orjson

YMMV as this is dependant on the objects being handled
"""

try:
    import orjson
except ImportError:
    import json as orjson

try:
    import ujson
except ImportError:
    import json as ujson


def dict_to_json(dic):
    serialized = orjson.dumps(dic)
    if not type(serialized).__name__ == "str":
        serialized = serialized.decode()
    return serialized


def json_to_dict(string):
    return ujson.loads(string)
