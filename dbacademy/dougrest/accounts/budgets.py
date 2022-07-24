from dbacademy.dougrest.accounts.crud import CRUD


class Budgets(CRUD):
    def __init__(self, accounts):
        super().__init__(accounts, "/budget", "budget")
