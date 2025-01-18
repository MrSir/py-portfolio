from datetime import datetime
from typing import Annotated

import typer
from typer import Typer

from pyp.cli.common import resolve_portfolio
from pyp.cli.portfolio.commands import AddMonikerCommand, AddSharesCommand
from pyp.database.engine import engine

portfolio_app = Typer(name="portfolio", help="Manage portfolio DB entities.")


@portfolio_app.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio name.")],
) -> None:
    ctx.obj = {"portfolio": resolve_portfolio(username, portfolio_name)}


@portfolio_app.command(name="add", help="Add a moniker to the portfolio.")
def add(ctx: typer.Context, moniker: Annotated[str, typer.Argument(help="The moniker.")]) -> None:
    AddMonikerCommand(engine, ctx.obj["portfolio"], moniker).execute()


@portfolio_app.command(name="add-shares", help="Add shares for a moniker to the portfolio.")
def add_shares(
    ctx: typer.Context,
    moniker: Annotated[str, typer.Argument(help="The moniker.")],
    amount: Annotated[float, typer.Argument(help="The amount of shares.")],
    price: Annotated[float, typer.Argument(help="The price paid for the shares.")],
    purchased_on: Annotated[datetime, typer.Argument(metavar="YYYY-MM-DD", help="The date of purchase of the shares.")],
) -> None:
    AddSharesCommand(engine, ctx.obj["portfolio"], moniker, amount, price, purchased_on).execute()
