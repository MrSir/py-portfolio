from typing import Annotated

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import User, Portfolio

portfolio_app = Typer(name="portfolio", help="Manage portfolio DB entities.")


@portfolio_app.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
) -> None:
    with Session(engine) as session:
        user = session.scalars(select(User).where(User.username == username)).one()

        ctx.obj = {"user": user}


@portfolio_app.command(name="create", help="Creates a portfolio for a specific user.")
def create(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="The portfolio to which to add the moniker.")],
) -> None:
    print(ctx.obj["user"])
    with Session(engine) as session:
        portfolio = Portfolio(user=ctx.obj["user"], name=name)

        session.add(portfolio)

        session.commit()


@portfolio_app.command(name="add-moniker", help="Add a moniker to the portfolio.")
def add(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="The portfolio to which to add the moniker.")],
    moniker: Annotated[str, typer.Argument(help="The moniker.")],
) -> None:
    pass


@portfolio_app.command(name="delete-moniker", help="Delete a moniker from the portfolio.")
def delete(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="The portfolio to which to add the moniker.")],
    moniker: Annotated[str, typer.Argument(help="The moniker.")],
) -> None:
    pass
