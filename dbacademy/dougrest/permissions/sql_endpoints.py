from dbacademy.dougrest import DatabricksApi
from dbacademy.dougrest.permissions.crud import PermissionsCrud


class SqlEndpoints(PermissionsCrud):
    valid_permissions = ["CAN_USE", "CAN_MANAGE"]

    def __init__(self, client: DatabricksApi):
        super().__init__(client, "2.0/permissions/sql/endpoints", "endpoints")
