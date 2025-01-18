from sqlalchemy import select
from sqlalchemy.orm import Session

from pyp.database.engine import engine
from pyp.database.models import Portfolio, User


def resolve_portfolio(username: str, portfolio_name: str) -> Portfolio:
    with Session(engine) as session:
        portfolio: Portfolio = session.scalars(
            select(Portfolio)
            .where(Portfolio.name == portfolio_name)
            .where(Portfolio.user.has(User.username == username))
        ).one()

    return portfolio
