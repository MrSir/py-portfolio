import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame
from sqlalchemy import Selectable, func, select

from pyp.cli.plot.commands.base import PlotCommand
from pyp.database.models import PortfolioStocks, Price, Share, Stock


class PlotGrowth(PlotCommand):
    @property
    def db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Share.amount,
                Share.price,
                Share.purchased_on,
                (
                    select(Price.amount)
                    .where(Price.stock_id == Stock.id)
                    .where(func.strftime("%Y-%m", Price.date) == func.strftime("%Y-%m", Share.purchased_on))
                    .order_by(Price.date.desc())
                    .limit(1)
                    .scalar_subquery()
                ).label("market_price"),
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .where(Share.purchased_on <= self.date.strftime("%Y-%m-%d"))
        )

    @property
    def db_data_df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "amount": "float64",
            "price": "float64",
            "market_price": "float64",
            "purchased_on": "object",
        }

    @property
    def invested_df(self) -> DataFrame:
        df = self.share_value_df.copy(deep=True)

        return df.rename(columns={"value": "invested"})

    @property
    def month_df(self) -> DataFrame:
        df = self.invested_df.copy(deep=True)
        df["month"] = pd.to_datetime(df["purchased_on"]).dt.strftime("%Y-%m")

        return df.drop(columns=["purchased_on"])

    @property
    def monthly_df(self) -> DataFrame:
        df = self.month_df.copy(deep=True)

        return (
            df.groupby(["moniker", "month"])
            .agg({
                "amount": "sum",
                "invested": "sum",
                "market_price": "last",
            })
            .reset_index()
        )

    @property
    def market_value_df(self) -> DataFrame:
        df = self.monthly_df.copy(deep=True)
        df["value"] = df["amount"] * df["market_price"]

        return df.drop(columns=["amount", "market_price"])

    @property
    def summed_monthly_df(self) -> DataFrame:
        df = self.market_value_df.copy(deep=True)

        return (
            df.groupby(["month"])
            .agg({
                "invested": "sum",
                "value": "sum",
            })
            .reset_index()
        )

    @property
    def profit_df(self) -> DataFrame:
        df = self.summed_monthly_df.copy(deep=True)
        df["profit"] = df["value"] - df["invested"]

        return df

    @property
    def growth_df(self) -> DataFrame:
        df = self.profit_df.copy(deep=True)
        df["profit_ratio"] = df["profit"] / df["invested"]

        return df

    def write_json_files(self) -> None:
        assert self.output_dir is not None

        self.growth_df.to_json(self.output_dir / "growth.json", orient="records")

    def show(self) -> None:
        df = self.growth_df

        invested_vs_value_df = df[["month", "invested", "value"]].copy(deep=True)
        invested_vs_value_df.plot.area(x="month", stacked=False, title="Invested vs. Market")

        profit_df = df[["month", "profit"]].copy(deep=True)
        profit_df.plot.area(x="month", stacked=False, title="Profit Growth")

        profit_ratio_df = df[["month", "profit_ratio"]].copy(deep=True)
        profit_ratio_df.plot.bar(x="month", title="Profit Ratio")

        plt.show()
