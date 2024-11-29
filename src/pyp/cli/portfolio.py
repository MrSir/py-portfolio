from typing import Annotated

import typer
from typer import Typer

portfolio = Typer(name="portfolio", help="Manage portfolio DB entities.")


@portfolio.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
) -> None:
    pass


@portfolio.command(
    name="create",
    help="Creates a portfolio for a specific user."
)
def create(ctx: typer.Context) -> None:
    pass


@portfolio.command(
    name="add-moniker",
    help="Add a moniker to the portfolio."
)
def add(
    ctx: typer.Context,
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio to which to add the moniker.")],
    moniker: Annotated[str, typer.Argument(help="The moniker.")],
) -> None:
    pass


@portfolio.command(
    name="delete-moniker",
    help="Delete a moniker from the portfolio."
)
def delete(
    ctx: typer.Context,
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio to which to add the moniker.")],
    moniker: Annotated[str, typer.Argument(help="The moniker.")],
) -> None:
    pass
