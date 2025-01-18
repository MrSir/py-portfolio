from typing import Annotated

import typer
from typer import Typer

from pyp.cli.user.commands import AddPortfolioCommand, AddUserCommand
from pyp.database.engine import engine

user_app = Typer(name="user", help="Manage user DB entities.")


@user_app.command(name="add", help="Creates a new user in the system.")
def add(username: Annotated[str, typer.Argument(help="The username of the user.")]) -> None:
    AddUserCommand(engine, username).execute()


@user_app.command(name="add-portfolio", help="Creates a portfolio for a specific user.")
def add_portfolio(
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    name: Annotated[str, typer.Argument(help="The portfolio name.")],
) -> None:
    AddPortfolioCommand(engine, username, name).execute()
