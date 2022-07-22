from dbacademy.dougrest.common import DatabricksApiException
from dbacademy.dougrest.accounts import AccountsApi
from dbacademy.dougrest.workspace import DatabricksApi


def _create_default_client():
    from dbacademy.dougrest import DatabricksApi
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
        host = config['DEFAULT']['host'].rstrip("/")[8:]
        token = config['DEFAULT']['token']
        return DatabricksApi(host, token=token)
    return DatabricksApi()


databricks = _create_default_client()
