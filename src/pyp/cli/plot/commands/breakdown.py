import json
from functools import cached_property
from typing import cast

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Connection, Selectable, select
from sqlalchemy.orm import Session

from pyp.database.engine import engine
from pyp.database.models import PortfolioStocks, Share, Stock


class PlotBreakdown:
    def __init__(self, portfolio_id: int):
        self.portfolio_id = portfolio_id

    @property
    def db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Stock.stock_type,
                Stock.sector_weightings,
                Share.amount,
                Share.price,
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
        )

    @cached_property
    def db_data_df(self) -> DataFrame:
        with Session(engine) as session:
            db_data_df = pd.read_sql(self.db_query, cast(Connection, session.bind)).astype(
                dtype={
                    "moniker": "string",
                    "stock_type": "string",
                    "sector_weightings": "object",
                    "amount": "float64",
                    "price": "float64",
                }
            )

        return db_data_df

    @property
    def share_value_df(self) -> DataFrame:
        df = self.db_data_df.copy(deep=True)
        df["value"] = df["amount"] * df["price"]

        return df.drop(columns=["amount", "price"])

    @property
    def group_by_moniker_df(self) -> DataFrame:
        return self.share_value_df.groupby(["moniker", "stock_type", "sector_weightings"]).sum().reset_index()

    @property
    def expand_by_sector_df(self) -> DataFrame:
        df = self.group_by_moniker_df.copy(deep=True)
        df = df.join(DataFrame(df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))

        return df.drop(columns=["sector_weightings"])

    @property
    def percent_by_moniker_df(self) -> DataFrame:
        df = self.expand_by_sector_df.copy(deep=True)
        df["total_value"] = df["value"].sum()
        df["percent"] = df["value"] / df["total_value"]

        return df.drop(columns=["value", "total_value"])

    @property
    def moniker_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)

        return df[["moniker", "percent"]]

    @property
    def stock_type_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)

        return df[["stock_type", "percent"]].groupby("stock_type").sum()

    @property
    def sector_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)
        df = df.drop(columns=["moniker", "stock_type"])
        df_minus_percent = df.drop(columns="percent")
        df = df_minus_percent.multiply(df["percent"], axis="index")
        df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"}).set_index("sector")

        return df

    def plot(self) -> None:
        pass
