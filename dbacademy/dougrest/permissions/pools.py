# from dbacademy.dougrest import DatabricksApi
from dbacademy.dougrest.permissions.crud import PermissionsCrud
from dbacademy.rest.common import ApiClient


class Pools(PermissionsCrud):
    valid_permissions = ["CAN_MANAGE", "CAN_ATTACH_TO"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/instance-pools", "instance_pool")
