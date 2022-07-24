from dbacademy.dougrest.accounts.crud import CRUD


class NetworkConfigurations(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/networks", "network")
