from functools import cached_property

import pandas as pd
from pandas import DataFrame
from sqlalchemy import select, Selectable
from sqlalchemy.orm import Session

from pyp.database.engine import engine
from pyp.database.models import Portfolio, Share, PortfolioStocks, Stock, Currency


class PlotBreakdown:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio

    @property
    def db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Stock.stock_type,
                Stock.sector_weightings,
                Share.amount,
                Share.price,
                Share.purchased_on,
                Currency.name.label("currency"),
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .join(Stock.currency)
            .where(PortfolioStocks.portfolio_id == self.portfolio.id)
        )

    @cached_property
    def db_data_df(self) -> DataFrame:
        with Session(engine) as session:
            assert session.bind

            db_data_df = pd.read_sql(self.db_query, session.bind)

        return db_data_df

    def plot(self) -> None:
        pass
