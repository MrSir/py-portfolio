from functools import cached_property
from typing import cast

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Connection, Selectable, select
from sqlalchemy.orm import Session

from pyp.database.engine import engine
from pyp.database.models import Currency, Portfolio, PortfolioStocks, Share, Stock


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
            db_data_df = pd.read_sql(self.db_query, cast(Connection, session.bind))

        return db_data_df

    @property
    def share_value_df(self) -> DataFrame:
        share_value_df = self.db_data_df.copy(deep=True)
        share_value_df["value"] = share_value_df["amount"] * share_value_df["price"]

        return share_value_df.drop(columns=["amount", "price"])

    def plot(self) -> None:
        pass
