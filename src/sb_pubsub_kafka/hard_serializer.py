import datetime
import inspect
import json
import uuid
from enum import Enum
from typing import get_type_hints, get_args


class HardSerializer(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        if "naming" in kwargs:
            self.naming = kwargs.pop("naming")
        super().__init__(*args, **kwargs)

    def serialize(self, obj, pretty: bool = False):
        d = self.map_to_dict(obj)
        return json.dumps(d, cls=HardSerializer, indent="\t" if pretty else None)

    def map_to_dict(self, obj):
        if isinstance(obj, list):
            new_list = []
            for item in obj:
                new_item = self.map_to_dict(item)
                new_list.append(new_item)
            return new_list

        if hasattr(obj, "map_to_dict"):
            return obj.map_to_dict(self)

        if isinstance(obj, uuid.UUID):
            return str(obj)

        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')

        if isinstance(obj, (str, int, float, bool)):
            return obj

        if isinstance(obj, Enum):
            return str(obj).replace("{}.".format(type(obj).__name__), "")

        if not isinstance(obj, dict):
            d = {}
            # this is to handle base class members
            for m in [
                m
                for m in inspect.getmembers(obj)
                if not m[0].startswith("_")
                   and not inspect.ismethod(m[1])
                   and not inspect.isfunction(m[1])
                   and not inspect.isbuiltin(m[1])
            ]:
                d[m[0]] = m[1]
        else:
            d = obj

        for child in d:
            d[child] = self.map_to_dict(d[child])
        return d

    def de_serialize(self, json_data, cls):
        dct = json.loads(json_data)
        obj = self.map_to_object(dct, cls)
        return obj

    def map_to_object(self, source_obj, cls):
        if type(source_obj) is list:
            new_list = []
            element_cls = get_args(cls)
            for item in source_obj:
                new_item = self.map_to_object(item, cls=element_cls[0])
                new_list.append(new_item)
            return new_list

        if cls == uuid.UUID:
            if isinstance(source_obj, uuid.UUID):
                return source_obj
            else:
                return uuid.UUID(source_obj)

        if cls == datetime.datetime:
            # 2023-10-12 15:49:22.634236
            return datetime.datetime.strptime(source_obj, '%Y-%m-%d %H:%M:%S.%f')

        if cls in (str, int, float, bool):
            return source_obj

        new_obj = cls()
        if hasattr(new_obj, "map_to_object"):
            new_obj.map_to_object(source_obj, self, self.naming)
            return new_obj

        if not isinstance(source_obj, dict):
            if isinstance(cls, type(None)):
                return source_obj
            else:
                return cls(source_obj)

        type_hint = get_type_hints(cls)

        for key in type_hint.keys():
            prop_cls = type_hint.get(key)
            value = source_obj.get(key, "")
            new_obj.__dict__[key] = self.map_to_object(value, prop_cls)

        return new_obj
