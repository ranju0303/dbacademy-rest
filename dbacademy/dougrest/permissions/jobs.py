from dbacademy.dougrest.permissions.crud import PermissionsCrud
from dbacademy.rest.common import ApiContainer, ApiClient


class Jobs(PermissionsCrud):

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/jobs", "job")
