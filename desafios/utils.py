import json


def get_dict_keys(d, schema=False, keys=None):
    if keys is None:
        keys = []
    for i, v in d.items():
        if isinstance(v, dict) and i not in ['type']:
            if i not in ['fields']:
                keys.append(i)
            get_dict_keys(v, schema, keys)
        elif i not in ['type', 'fields']:
            keys.append(i)
    return keys


def get_schema(schema):
    with open(schema) as sch:
        return json.loads(sch.read())
