from typing import Sequence

import pandas as pd
from pandas import DataFrame
from sqlalchemy import select
from sqlalchemy.orm import Session

from pyp.database.engine import engine
from pyp.database.models import Portfolio, Share, PortfolioStocks, Stock


class PlotBreakdown:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio

    @property
    def shares_df(self) -> DataFrame:
        with Session(engine) as session:
            session.add(self.portfolio)

            query = select(Share).where(Share.portfolio_stocks.has(PortfolioStocks.portfolio == self.portfolio))

            assert session.bind

            shares_df = pd.read_sql(query, session.bind)

        return shares_df

    @property
    def portfolio_stocks_df(self) -> DataFrame:
        with Session(engine) as session:
            session.add(self.portfolio)

            query = select(PortfolioStocks).where(PortfolioStocks.portfolio == self.portfolio)

            assert session.bind

            portfolio_stocks_df = pd.read_sql(query, session.bind)

        return portfolio_stocks_df

    @property
    def stocks_df(self) -> DataFrame:
        with Session(engine) as session:
            session.add(self.portfolio)

            query = select(Stock).where(Stock.portfolio_stocks.any(PortfolioStocks.portfolio == self.portfolio))

            assert session.bind

            stocks_df = pd.read_sql(query, session.bind)

        return stocks_df

    @property
    def combined_df(self) -> DataFrame:
        combined_df = self.shares_df.merge(
            self.portfolio_stocks_df,
            left_on="portfolio_stocks_id",
            right_on="id",
        ).drop(columns=["id_x", "id_y", "portfolio_stocks_id"])

        combined_df = combined_df.merge(
            self.stocks_df,
            left_on="stock_id",
            right_on="id",
        ).drop(columns=["portfolio_id", "stock_id", "id", "purchased_on", "name", "description"])

        combined_df["value"] = combined_df["amount"] * combined_df["price"]

        combined_df = combined_df.drop(columns=["amount", "price"])

        return combined_df

    @property
    def stock_type_df(self) -> DataFrame:
        stock_type_df = self.combined_df.drop(columns=["sector_weightings", "moniker"])

        return stock_type_df.groupby("stock_type").agg("sum")

    def plot(self) -> None:
        print(self.stock_type_df)
