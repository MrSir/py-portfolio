from typing import Annotated

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import Portfolio, User

user_app = Typer(name="user", help="Manage user DB entities.")


@user_app.command(name="add", help="Creates a new user in the system.")
def add(username: Annotated[str, typer.Argument(help="The username of the user.")]) -> None:
    with Session(engine) as session:
        user = User(username=username)

        session.add(user)

        session.commit()


@user_app.command(name="add-portfolio", help="Creates a portfolio for a specific user.")
def add_portfolio(
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    name: Annotated[str, typer.Argument(help="The portfolio name.")],
) -> None:
    with Session(engine) as session:
        user = session.scalars(select(User).where(User.username == username)).one()
        portfolio = Portfolio(name=name)

        user.portfolios.append(portfolio)

        session.add(user)

        session.commit()
