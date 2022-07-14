def load_dbgems():
  """
  Hack to work around module import problems.
  """
  module_name="dbacademy.dbgems"
  rel_path="dbacademy/dbgems/__init__.py"
  try:
    import dbacademy
    import dbacademy.dbgems
    if hasattr(dbacademy, "dbgems"):
      return dbacademy.dbgems
  except ModuleNotFoundError as e:
    pass
  import sys
  from os.path import exists
  from importlib.util import spec_from_file_location, module_from_spec
  for dir in sys.path:
    path=dir + "/" + rel_path
    if exists(path):
      break
  spec = spec_from_file_location(module_name, path)
  module = module_from_spec(spec)
  sys.modules[module_name] = module
  spec.loader.exec_module(module)
  try:
    import dbacademy
    dbacademy.dbgems=module
  except ModuleNotFoundError as e:
    pass
  return module

load_dbgems()


def create_client():
  from dbacademy.dbrest import DBAcademyRestClient
  import os
  import configparser

  for path in ('.databrickscfg', '~/.databrickscfg'):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
      continue
    config = configparser.ConfigParser()
    config.read(path)
    if not 'DEFAULT' in config:
      print('No Default')
      continue
    host = config['DEFAULT']['host'].rstrip("/")
    token = config['DEFAULT']['token']
    return DBAcademyRestClient(token, host)
  return DBAcademyRestClient()

databricks = create_client()

if __name__ == '__main__':
  from dbacademy.dbrest.tests.all import main
  main()
