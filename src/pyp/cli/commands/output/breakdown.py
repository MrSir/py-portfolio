import json
from sqlite3 import Connection
from typing import Self, cast

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Selectable, func, select
from sqlalchemy.orm import Session

from pyp.cli.commands.output.base import OutputCommand
from pyp.database.models import ExchangeRate, PortfolioStocks, Price, Share, Stock


class OutputBreakdownCommand(OutputCommand):
    @property
    def _db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Stock.stock_type,
                Stock.currency_id,
                Stock.sector_weightings,
                func.sum(Share.amount).label("amount"),
                (
                    select(Price.amount)
                    .select_from(Price)
                    .where(Price.stock_id == Stock.id)
                    .where(Price.date <= self.date.strftime("%Y-%m-%d"))
                    .order_by(Price.date.desc())
                    .limit(1)
                    .scalar_subquery()
                ).label("price"),
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .where(Share.purchased_on <= self.date.strftime("%Y-%m-%d"))
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .group_by(Stock.moniker)
        )

    @property
    def _df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "stock_type": "string",
            "currency_id": "int64",
            "sector_weightings": "object",
            "amount": "float64",
            "price": "float64",
        }

    @property
    def _exchange_rates_query(self) -> Selectable:
        base_currency_id = self._currency_ids_by_code[self.currency_code]
        currency_ids = [
            currency_id for currency_id in self._currency_ids_by_code.values() if currency_id != base_currency_id
        ]

        return (
            select(
                ExchangeRate.from_currency_id,
                ExchangeRate.to_currency_id,
                func.max(ExchangeRate.date).label("date"),
                ExchangeRate.rate,
            )
            .select_from(ExchangeRate)
            .where(ExchangeRate.to_currency_id == base_currency_id)
            .where(ExchangeRate.from_currency_id.in_(currency_ids))
            .where(ExchangeRate.date <= self.date.strftime("%Y-%m-%d"))
            .group_by(ExchangeRate.from_currency_id, ExchangeRate.to_currency_id)
        )

    @property
    def _exchange_rates_df(self) -> Self:
        with Session(self.engine) as session:
            df = pd.read_sql(self._exchange_rates_query, cast(Connection, session.bind)).astype(
                dtype={
                    "from_currency_id": "int64",
                    "to_currency_id": "int64",
                    "date": "string",
                    "rate": "float64",
                }
            )

        return df

    def _add_exchange_rates(self) -> Self:
        self._df = self._df.merge(
            self._exchange_rates_df,
            how="left",
            left_on=["currency_id"],
            right_on=["from_currency_id"],
        )

        self._df["rate"] = self._df["rate"].fillna(1)

        self._df = self._df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

        return self

    def _convert_to_currency(self) -> Self:
        self._df["amount"] = self._df["amount"] * self._df["rate"]
        self._df["value"] = self._df["value"] * self._df["rate"]

        self._df = self._df.drop(columns=["rate", "date"])

        return self

    def _expand_by_sector(self) -> Self:
        self._df["sector_weightings"] = self._df["sector_weightings"].str.replace("_", "-")

        self._df = (
            self._df.join(DataFrame(self._df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))
        ).drop(columns=["amount", "sector_weightings"])

        return self

    def _calculate_percent_by_moniker(self) -> Self:
        self._df["total_value"] = self._df["value"].sum()
        self._df["percent"] = self._df["value"] / self._df["total_value"]
        self._df = self._df.drop(columns=["value", "total_value"])

        return self

    @property
    def _moniker_breakdown_df(self) -> DataFrame:
        return self._df[["moniker", "percent"]]  # type:ignore[index]

    @property
    def _stock_type_breakdown_df(self) -> DataFrame:
        return self._df[["stock_type", "percent"]].groupby("stock_type").sum().reset_index()  # type:ignore[index]

    @property
    def _sector_breakdown_df(self) -> DataFrame:
        df = self._df.drop(columns=["moniker", "stock_type"])  # type:ignore[union-attr]
        df = df.drop(columns="percent").multiply(df["percent"], axis="index")
        df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

        return df

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._resolve_currency_ids()
            ._add_exchange_rates()
            ._convert_to_currency()
            ._expand_by_sector()
            ._calculate_percent_by_moniker()
        )

    def _write_data_files(self) -> None:
        by_moniker_json_string = self._moniker_breakdown_df.to_json(orient="records")
        by_stock_type_json_string = self._stock_type_breakdown_df.to_json(orient="records")
        by_sector_json_string = self._sector_breakdown_df.to_json(orient="records")

        with open(self.output_dir / "breakdown.js", "w") as file:
            file.write(f"breakdown_by_moniker_data = {by_moniker_json_string}\n")
            file.write(f"breakdown_by_stock_type_data = {by_stock_type_json_string}\n")
            file.write(f"breakdown_by_sector_data = {by_sector_json_string}\n")
