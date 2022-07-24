from dbacademy.dougrest.scim.groups import Groups
from dbacademy.dougrest.scim.users import Users


class SCIM(object):
    def __init__(self, databricks):
        self.databricks = databricks
        self.users = Users(databricks)
        self.groups = Groups(databricks)
