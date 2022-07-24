from dbacademy.dougrest.accounts.crud import CRUD


class VpcEndpoints(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/vpc-endpoints", "vpc_endpoint")
