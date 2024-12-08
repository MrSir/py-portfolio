import json
from functools import cached_property
from pathlib import Path
from typing import cast

import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame
from sqlalchemy import Connection, Selectable, select
from sqlalchemy.orm import Session

from pyp.database.engine import engine
from pyp.database.models import PortfolioStocks, Share, Stock


class PlotBreakdown:
    def __init__(self, portfolio_id: int, output_dir: Path | None = None):
        self.portfolio_id = portfolio_id
        self.output_dir = output_dir
        self.show = self.output_dir is None

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

        return df[["stock_type", "percent"]].groupby("stock_type").sum().reset_index()

    @property
    def sector_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)
        df = df.drop(columns=["moniker", "stock_type"])
        df_minus_percent = df.drop(columns="percent")
        df = df_minus_percent.multiply(df["percent"], axis="index")
        df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

        return df

    def write_json_files(self) -> None:
        assert self.output_dir is not None

        self.moniker_breakdown_df.to_json(self.output_dir / "moniker_breakdown.json", orient="records")
        self.stock_type_breakdown_df.to_json(self.output_dir / "stock_type_breakdown.json", orient="records")
        self.sector_breakdown_df.to_json(self.output_dir / "sector_breakdown.json", orient="records")

    def show_breakdowns(self) -> None:
        self.moniker_breakdown_df.plot.pie(
            y="percent", labels=self.moniker_breakdown_df["moniker"], figsize=(5, 5), autopct="%.2f%%"
        )
        self.stock_type_breakdown_df.plot.pie(
            y="percent", labels=self.stock_type_breakdown_df["stock_type"], figsize=(5, 5), autopct="%.2f%%"
        )
        self.sector_breakdown_df.plot.pie(
            y="percent", labels=self.sector_breakdown_df["sector"], figsize=(5, 5), autopct="%.2f%%"
        )

        plt.show()

    def plot(self) -> None:
        if self.output_dir is not None:
            self.write_json_files()

            return

        self.show_breakdowns()
