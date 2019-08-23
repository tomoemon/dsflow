def process(entity):
    entity["ScriptBody"] = entity["ScriptBody"].decode('utf-8')
    return [entity]
