from dbacademy.dougrest.accounts.crud import CRUD


class Credentials(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/credentials", "credentials", "credential")
