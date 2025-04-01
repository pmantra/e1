import orjson


def sanitize_json_input(json):
    """converts input json to string"""
    if type(json) is str:
        return json
    elif type(json) is set:
        return orjson.dumps(list(json))
    else:
        return orjson.dumps(json)
