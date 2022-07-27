from dbacademy.rest.common import ApiClient
from dbacademy.rest.permissions.crud import PermissionsCrud

__all__ = ["Jobs"]


class Jobs(PermissionsCrud):
    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/jobs", "job")
