"""
Hack to work around module import problems.
"""


def _load_dbgems():
    module_name = "dbacademy.dbgems"
    rel_path = "dbacademy/dbgems/__init__.py"
    try:
        import dbacademy
        import dbacademy.dbgems
        if hasattr(dbacademy, "dbgems"):
            return dbacademy.dbgems
    except ModuleNotFoundError:
        pass
    import sys
    from os.path import exists
    from importlib.util import spec_from_file_location, module_from_spec
    for path in sys.path:
        path = path + "/" + rel_path
        if exists(path):
            break
    else:
        raise ModuleNotFoundError("dbacademy.dbgems not found")
    spec = spec_from_file_location(module_name, path)
    module = module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    dbacademy.dbgems = module
    return module


dbgems = _load_dbgems()
