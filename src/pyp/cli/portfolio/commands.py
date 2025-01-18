from datetime import datetime

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from pyp.database.models import Portfolio, PortfolioStocks, Share, Stock


class AddMonikerCommand:
    def __init__(self, engine: Engine, portfolio: Portfolio, moniker: str):
        self.engine = engine
        self.portfolio = portfolio
        self.moniker = moniker

        self._stock: Stock | None = None

    def _resolve_stock(self) -> Stock | None:
        with Session(self.engine) as session:
            stock = session.scalars(select(Stock).where(Stock.moniker == self.moniker)).first()

        return stock

    def _create_stock(self) -> Stock:
        with Session(self.engine) as session:
            stock = Stock(moniker=self.moniker)
            session.add(stock)
            session.commit()

        return stock

    @property
    def stock(self) -> Stock:
        if self._stock is None:
            resolved_stock = self._resolve_stock()
            self._stock = resolved_stock if resolved_stock is not None else self._create_stock()

        return self._stock

    def execute(self) -> None:
        with Session(self.engine) as session:
            session.add(self.portfolio)
            self.portfolio.stocks.append(self.stock)

            session.commit()


class AddSharesCommand:
    _portfolio_stocks: PortfolioStocks
    _share: Share

    def __init__(
        self,
        engine: Engine,
        portfolio: Portfolio,
        moniker: str,
        amount: float,
        price: float,
        purchased_on: datetime,
    ):
        self.engine = engine
        self.portfolio = portfolio
        self.moniker = moniker
        self.amount = amount
        self.price = price
        self.purchased_on = purchased_on

    def _resolve_portfolio_stocks(self) -> None:
        with Session(self.engine) as session:
            session.add(self.portfolio)

            self._portfolio_stocks = session.scalars(
                select(PortfolioStocks)
                .where(PortfolioStocks.portfolio == self.portfolio)
                .where(PortfolioStocks.stock.has(Stock.moniker == self.moniker))
            ).one()

    def _prepare_share(self) -> None:
        self._share = Share(
            amount=self.amount,
            price=self.price,
            purchased_on=self.purchased_on,
        )

    def execute(self) -> None:
        self._resolve_portfolio_stocks()
        self._prepare_share()

        with Session(self.engine) as session:
            session.add(self._portfolio_stocks)

            self._portfolio_stocks.shares.append(self._share)

            session.commit()
