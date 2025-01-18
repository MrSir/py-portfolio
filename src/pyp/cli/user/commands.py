from sqlalchemy import Engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pyp.database.models import Portfolio, User


class AddUserCommand:
    _user: User

    def __init__(self, engine: Engine, username: str):
        self.engine = engine
        self.username = username

    def _prepare_user(self) -> None:
        self._user = User(username=self.username)

    def execute(self) -> None:
        self._prepare_user()

        with Session(self.engine) as session:
            session.add(self._user)

            try:
                session.commit()
            except IntegrityError:
                print(f"The user '{self.username}' already exists.")


class AddPortfolioCommand:
    _user: User
    _portfolio: Portfolio

    def __init__(self, engine: Engine, username: str, name: str):
        self.engine = engine
        self.username = username
        self.name = name

    def _prepare_portfolio(self) -> None:
        self._portfolio = Portfolio(name=self.name)

    def _resolve_user(self) -> None:
        with Session(self.engine) as session:
            self._user = session.scalars(select(User).where(User.username == self.username)).one()

    def execute(self) -> None:
        self._prepare_portfolio()
        self._resolve_user()

        with Session(self.engine) as session:
            session.add(self._user)

            self._user.portfolios.append(self._portfolio)

            try:
                session.commit()
            except IntegrityError:
                print(f"The portfolio '{self.name}' already exists, for user '{self.username}'.")
