import json
from datetime import datetime
from typing import Self

import numpy as np
from sqlalchemy import Engine

from pyp.cli.commands.output.growth import OutputGrowthCommand


class OutputSummaryCommand(OutputGrowthCommand):
    def __init__(
        self, engine: Engine, username: str, portfolio_name: str, portfolio_id: int, date: datetime, currency_code: str
    ):
        self.username = username
        self.portfolio_name = portfolio_name

        super().__init__(engine, portfolio_id, date, currency_code)

    def _compute_market_value(self) -> Self:
        self._df["value"] = self._df["amount"] * self._df["market_price"]

        return self

    def _convert_to_currency(self) -> Self:
        self._df["market_price"] = self._df["market_price"] * self._df["rate"]
        self._df["invested"] = self._df["invested"] * self._df["rate"]
        self._df["value"] = self._df["value"] * self._df["rate"]

        self._df = self._df.drop(columns=["rate"])

        return self

    def _sum_up_by_moniker(self) -> Self:
        self._df = (
            self._df.groupby(["moniker"], sort=False)
            .agg({
                "stock_type": "first",
                "invested": "sum",
                "amount": "last",
                "market_price": "last",
                "value": "last",
            })
            .reset_index()
        )

        self._df = self._df[self._df["invested"] > 0]

        return self

    def _compute_average_price(self) -> Self:
        self._df["average_price"] = (self._df["invested"] / self._df["amount"]).replace([-np.inf], np.nan).fillna(0)

        return self

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._rename_value_to_invested()
            ._add_monthly_market_prices()
            ._compute_market_value()
            ._resolve_currency_ids()
            ._add_exchange_rates()
            ._convert_to_currency()
            ._sum_up_by_moniker()
            ._compute_average_price()
        )

    def _invested_value(self) -> float:
        return self._df["invested"].sum()

    def _market_value(self) -> float:
        return self._df["value"].sum()

    def _number_of_equities(self) -> int:
        return len(self._df[self._df["stock_type"] == "EQUITY"])

    def _number_of_etfs(self) -> int:
        return len(self._df[self._df["stock_type"] == "ETF"])

    def _write_data_files(self) -> None:
        invested_value = self._invested_value()
        market_value = self._market_value()

        summary = {
            "date": self.date.strftime("%d-%b-%Y"),
            "username": self.username,
            "portfolio": self.portfolio_name,
            "currency": self.currency_code,
            "invested": round(invested_value, 2),
            "value": round(market_value, 2),
            "percent": round(((market_value - invested_value) / invested_value) * 100, 2),
            "equities": self._number_of_equities(),
            "etfs": self._number_of_etfs(),
        }
        summary_json = json.dumps(summary)

        portfolio_json = self._df.to_json(orient="records")

        with open(self.output_dir / "summary.js", "w") as file:
            file.write(f"summary_data = {summary_json}\n")
            file.write(f"portfolio_data = {portfolio_json}\n")
