from typing import Self

import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame
from sqlalchemy import Selectable, func, select

from pyp.cli.plot.commands.base import PlotCommand
from pyp.database.models import PortfolioStocks, Price, Share, Stock


class PlotGrowth(PlotCommand):
    @property
    def _db_query(self) -> Selectable:
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
    def _df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "amount": "float64",
            "price": "float64",
            "market_price": "float64",
            "purchased_on": "object",
        }

    def _rename_value_to_invested(self) -> Self:
        if self._df is not None:
            self._df = self._df.rename(columns={"value": "invested"})

        return self

    def _parse_purchased_on_to_month(self) -> Self:
        if self._df is not None:
            self._df["month"] = pd.to_datetime(self._df["purchased_on"]).dt.strftime("%Y-%m")
            self._df = self._df.drop(columns=["purchased_on"])

        return self

    def _sum_up_by_month_and_moniker(self) -> Self:
        if self._df is not None:
            self._df = (
                self._df.groupby(["moniker", "month"])
                .agg({
                    "amount": "sum",
                    "invested": "sum",
                    "market_price": "last",
                })
                .reset_index()
            )

        return self

    def _add_missing_months(self) -> Self:
        # TODO Implement and Test
        # find the months from the timeline that are missing and add them
        # May need to move this earlier as it will need to query prices and use monikers
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
            self._df["cum_sum_value"] = self._df["value"].cumsum()
            self._df["cum_sum_invested"] = self._df["invested"].cumsum()

            self._df = self._df.drop(columns=["invested", "value"]).rename(
                columns={"cum_sum_value": "value", "cum_sum_invested": "invested"}
            )

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
        return self._df[["month", "profit_ratio"]]  # type:ignore[index]

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._rename_value_to_invested()
            ._parse_purchased_on_to_month()
            ._sum_up_by_month_and_moniker()
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
        self._invested_vs_value_df.plot.area(x="month", stacked=False, title="Invested vs. Market")
        self._month_vs_profit_df.plot.area(x="month", stacked=False, title="Profit Growth")
        self._month_vs_profit_ratio_df.plot.bar(x="month", title="Profit Ratio")

        plt.show()
