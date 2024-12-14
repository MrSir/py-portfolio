from typing import Self, cast

import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame
from sqlalchemy import Connection, Selectable, func, select
from sqlalchemy.orm import Session

from pyp.cli.plot.commands.base import PlotCommand
from pyp.database.engine import engine
from pyp.database.models import PortfolioStocks, Price, Share, Stock


class PlotGrowth(PlotCommand):
    @property
    def _db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Share.amount,
                Share.price,
                func.strftime("%Y-%m", Share.purchased_on).label("month"),
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .where(Share.purchased_on <= self.date.strftime("%Y-%m-%d"))
        )

    @property
    def _df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "amount": "float64",
            "price": "float64",
            "month": "string",
        }

    def _rename_value_to_invested(self) -> Self:
        if self._df is not None:
            self._df = self._df.rename(columns={"value": "invested"})

        return self

    def _sum_up_by_month_and_moniker(self) -> Self:
        if self._df is not None:
            self._df = (
                self._df.groupby(["moniker", "month"])
                .agg({
                    "amount": "sum",
                    "invested": "sum",
                })
                .reset_index()
            )

        return self

    @property
    def _monthly_prices_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                func.strftime("%Y-%m", func.max(Price.date)).label("month"),
                Price.amount.label("market_price"),
            )
            .select_from(Price)
            .join(Price.stock)
            .join(Stock.portfolio_stocks)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .where(Price.date <= self.date.strftime("%Y-%m-%d"))
            .group_by(Stock.moniker, func.strftime("%Y-%m", Price.date))
        )

    @property
    def _monthly_prices_df(self) -> DataFrame:
        with Session(engine) as session:
            df = pd.read_sql(self._monthly_prices_query, cast(Connection, session.bind)).astype(
                dtype={"moniker": "string", "month": "string", "market_price": "float64"}
            )

        return df

    def _add_monthly_market_prices(self) -> Self:
        if self._df is not None:
            self._df = self._monthly_prices_df.merge(
                self._df, how="left", left_on=["moniker", "month"], right_on=["moniker", "month"]
            )

            self._df[["amount", "invested"]] = self._df[["amount", "invested"]].fillna(0, axis="columns")
            self._df["amount"] = self._df.groupby("moniker")["amount"].cumsum()

        return self

    def _compute_market_value(self) -> Self:
        if self._df is not None:
            self._df["value"] = self._df["amount"] * self._df["market_price"]
            self._df = self._df.drop(columns=["amount", "market_price"])

        return self

    def _sum_up_by_month(self) -> Self:
        if self._df is not None:
            self._df = (
                self._df.groupby(["month"])
                .agg({
                    "invested": "sum",
                    "value": "sum",
                })
                .reset_index()
            )

        return self

    def _compute_cumulative_sums(self) -> Self:
        if self._df is not None:
            self._df["cum_sum_invested"] = self._df["invested"].cumsum()

            self._df = self._df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

        return self

    def _compute_profit(self) -> Self:
        if self._df is not None:
            self._df["profit"] = self._df["value"] - self._df["invested"]

        return self

    def _compute_profit_ratio(self) -> Self:
        if self._df is not None:
            self._df["profit_ratio"] = self._df["profit"] / self._df["invested"]

        return self

    @property
    def _invested_vs_value_df(self) -> DataFrame:
        return self._df[["month", "invested", "value"]]  # type:ignore[index]

    @property
    def _month_vs_profit_df(self) -> DataFrame:
        return self._df[["month", "profit"]]  # type:ignore[index]

    @property
    def _month_vs_profit_ratio_df(self) -> DataFrame:
        df = self._df[["month", "profit_ratio"]].copy(deep=True)  # type:ignore[index]
        df["profit_ratio"] = df["profit_ratio"] * 100

        return df

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._rename_value_to_invested()
            ._sum_up_by_month_and_moniker()
            ._add_monthly_market_prices()
            ._compute_market_value()
            ._sum_up_by_month()
            ._compute_cumulative_sums()
            ._compute_profit()
            ._compute_profit_ratio()
        )

    def _write_json_files(self) -> None:
        if self.output_dir is not None:
            self._df.to_json(self.output_dir / "growth.json", orient="records")  # type:ignore[union-attr]

    def _show(self) -> None:
        self._invested_vs_value_df.plot.area(
            x="month",
            stacked=False,
            title="Invested vs. Market",
            xlabel="Month",
            ylabel="Dollars ($)",
            grid=True,
            rot=90,
        )
        self._month_vs_profit_df.plot.area(
            x="month",
            stacked=False,
            title="Profit Growth",
            xlabel="Month",
            ylabel="Dollars ($)",
            grid=True,
            rot=90,
        )
        self._month_vs_profit_ratio_df.plot.bar(
            x="month",
            title="Profit Ratio",
            xlabel="Month",
            ylabel="Ratio",
            grid=True,
            rot=90,
        )

        plt.show()
