from typing import Annotated

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import User, Portfolio, Stock

portfolio_app = Typer(name="portfolio", help="Manage portfolio DB entities.")


@portfolio_app.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    name: Annotated[str, typer.Argument(help="The portfolio name.")],
) -> None:
    with Session(engine) as session:
        user: User = session.scalars(select(User).where(User.username == username)).one()
        portfolio: Portfolio = session.scalars(
            select(Portfolio).where(Portfolio.name == name).where(Portfolio.user_id == user.id)
        ).one()

        ctx.obj = {"user": user, "portfolio": portfolio}


@portfolio_app.command(name="add-moniker", help="Add a moniker to the portfolio.")
def add(ctx: typer.Context, moniker: Annotated[str, typer.Argument(help="The moniker.")]) -> None:
    with Session(engine) as session:
        portfolio: Portfolio = ctx.obj["portfolio"]
        session.add(portfolio)

        stock: Stock = session.scalars(select(Stock).where(Stock.moniker == moniker)).first()

        if stock is None:
            stock = Stock(moniker=moniker)
            session.add(stock)
            session.commit()

        portfolio.stocks.append(stock)

        session.commit()


@portfolio_app.command(name="delete-moniker", help="Delete a moniker from the portfolio.")
def remove(ctx: typer.Context, moniker: Annotated[str, typer.Argument(help="The moniker.")]) -> None:
    with Session(engine) as session:
        portfolio: Portfolio = ctx.obj["portfolio"]
        session.add(portfolio)

        stock: Stock = session.scalars(select(Stock).where(Stock.moniker == moniker)).first()

        portfolio.stocks.remove(stock)

        session.commit()
