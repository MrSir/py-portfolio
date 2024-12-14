from datetime import datetime
from typing import Annotated

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer import Typer

from pyp.cli.common import resolve_portfolio
from pyp.database.engine import engine
from pyp.database.models import Portfolio, PortfolioStocks, Share, Stock

portfolio_app = Typer(name="portfolio", help="Manage portfolio DB entities.")

# TODO Test


@portfolio_app.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio name.")],
) -> None:
    ctx.obj = {"portfolio": resolve_portfolio(username, portfolio_name)}


@portfolio_app.command(name="add", help="Add a moniker to the portfolio.")
def add(ctx: typer.Context, moniker: Annotated[str, typer.Argument(help="The moniker.")]) -> None:
    with Session(engine) as session:
        portfolio: Portfolio = ctx.obj["portfolio"]
        session.add(portfolio)

        stock = session.scalars(select(Stock).where(Stock.moniker == moniker)).first()

        if stock is None:
            stock = Stock(moniker=moniker)
            session.add(stock)
            session.commit()

        portfolio.stocks.append(stock)

        session.commit()


@portfolio_app.command(name="delete", help="Delete a moniker from the portfolio.")
def remove(ctx: typer.Context, moniker: Annotated[str, typer.Argument(help="The moniker.")]) -> None:
    with Session(engine) as session:
        portfolio: Portfolio = ctx.obj["portfolio"]
        session.add(portfolio)

        stock = session.scalars(select(Stock).where(Stock.moniker == moniker)).one()

        portfolio.stocks.remove(stock)

        session.commit()


@portfolio_app.command(name="add-shares", help="Add shares for a moniker to the portfolio.")
def add_shares(
    ctx: typer.Context,
    moniker: Annotated[str, typer.Argument(help="The moniker.")],
    amount: Annotated[float, typer.Argument(help="The amount of shares.")],
    price: Annotated[float, typer.Argument(help="The price paid for the shares.")],
    purchased_on: Annotated[datetime, typer.Argument(metavar="YYYY-MM-DD", help="The date of purchase of the shares.")],
) -> None:
    with Session(engine) as session:
        portfolio: Portfolio = ctx.obj["portfolio"]
        session.add(portfolio)

        portfolio_stocks = session.scalars(
            select(PortfolioStocks)
            .where(PortfolioStocks.portfolio == portfolio)
            .where(PortfolioStocks.stock.has(Stock.moniker == moniker))
        ).one()

        portfolio_stocks.shares.append(
            Share(
                amount=amount,
                price=price,
                purchased_on=purchased_on,
            )
        )

        session.commit()
