from datetime import datetime
from typing import Self, cast

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Connection, Selectable, func, select
from sqlalchemy.orm import Session

from pyp.cli.commands.output.base import OutputCommand
from pyp.database.models import PortfolioStocks, Price, Share, Stock


class OutputGrowthCommand(OutputCommand):
    @property
    def _db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Stock.stock_type,
                Share.amount,
                Share.price,
                func.strftime("%Y-%m", Share.purchased_on).label("month"),
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .where(Share.purchased_on <= self.date.strftime("%Y-%m-%d"))
            .order_by(Share.purchased_on)
        )

    @property
    def _df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "stock_type": "string",
            "amount": "float64",
            "price": "float64",
            "month": "string",
        }

    def _rename_value_to_invested(self) -> Self:
        self._df = self._df.rename(columns={"value": "invested"})

        return self

    def _sum_up_by_month_and_moniker(self) -> Self:
        self._df = (
            self._df.groupby(["moniker", "month"], sort=False)
            .agg({
                "stock_type": "first",
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
                Stock.stock_type,
                Stock.currency_id,
                func.strftime("%Y-%m", func.max(Price.date)).label("month"),
                Price.amount.label("market_price"),
            )
            .select_from(Price)
            .join(Price.stock)
            .join(Stock.portfolio_stocks)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .where(Price.date <= self.date.strftime("%Y-%m-%d"))
            .group_by(Stock.moniker, func.strftime("%Y-%m", Price.date))
            .order_by(Price.date)
        )

    @property
    def _monthly_prices_df(self) -> DataFrame:
        with Session(self.engine) as session:
            df = pd.read_sql(self._monthly_prices_query, cast(Connection, session.bind)).astype(
                dtype={
                    "moniker": "string",
                    "stock_type": "string",
                    "currency_id": "int64",
                    "month": "string",
                    "market_price": "float64",
                }
            )

        return df

    def _add_monthly_market_prices(self) -> Self:
        self._df = self._monthly_prices_df.merge(
            self._df,
            how="left",
            left_on=["moniker", "month", "stock_type"],
            right_on=["moniker", "month", "stock_type"],
        )

        self._df[["amount", "invested"]] = self._df[["amount", "invested"]].fillna(0, axis="columns")
        self._df["amount"] = self._df.groupby("moniker")["amount"].cumsum()

        self._df["month"] = self._df["month"].apply(lambda x: datetime.strptime(x, "%Y-%m").strftime("%b-%y"))

        return self

    def _compute_market_value(self) -> Self:
        self._df["value"] = self._df["amount"] * self._df["market_price"]
        self._df = self._df.drop(columns=["amount", "market_price"]).reset_index()

        return self

    def _convert_to_currency(self) -> Self:
        self._df["invested"] = self._df["invested"] * self._df["rate"]
        self._df["value"] = self._df["value"] * self._df["rate"]

        self._df = self._df.drop(columns=["rate"])

        return self

    def _sum_up_by_month(self) -> Self:
        self._df = (
            self._df.groupby(["month"], sort=False)
            .agg({
                "stock_type": "first",
                "invested": "sum",
                "value": "sum",
            })
            .reset_index()
        )

        return self

    def _compute_cumulative_sums(self) -> Self:
        self._df["cum_sum_invested"] = self._df["invested"].cumsum()

        self._df = self._df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

        return self

    def _compute_profit(self) -> Self:
        self._df["profit"] = self._df["value"] - self._df["invested"]

        return self

    def _compute_profit_ratio(self) -> Self:
        self._df["profit_ratio"] = (self._df["profit"] / self._df["invested"]).fillna(0)

        self._df.loc[self._df["invested"] <= 0, "profit_ratio"] = 0

        return self

    def _compute_profit_ratio_difference(self) -> Self:
        self._df["profit_ratio_difference"] = self._df["profit_ratio"] - self._df["profit_ratio"].shift()

        return self

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._rename_value_to_invested()
            ._sum_up_by_month_and_moniker()
            ._add_monthly_market_prices()
            ._compute_market_value()
            ._resolve_currency_ids()
            ._add_exchange_rates()
            ._convert_to_currency()
            ._sum_up_by_month()
            ._compute_cumulative_sums()
            ._compute_profit()
            ._compute_profit_ratio()
            ._compute_profit_ratio_difference()
        )

    def _write_data_files(self) -> None:
        json_string = self._df.drop(columns=["stock_type"]).to_json(orient="records")

        with open(self.output_dir / "growth.js", "w") as file:
            file.write(f"growth_data = {json_string}")
