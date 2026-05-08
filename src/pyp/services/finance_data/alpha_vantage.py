import http
import json
from datetime import datetime
from typing import Any, Sequence

import requests
from dotenv import dotenv_values
from pandas import DataFrame

from pyp.database.models import Currency, Stock
from pyp.services.finance_data.base import FinanceDataService


# https://www.alphavantage.co/documentation/
# 25 request/day
class AlphaVantageFinanceDataService(FinanceDataService):
    def __init__(
        self,
        stocks: Sequence[Stock],
        currencies_by_code: dict[str, Currency],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        super().__init__(stocks, currencies_by_code, start_date, end_date)

        self._api_key: str | None = None

    @property
    def api_key(self) -> str:
        if self._api_key is None:
            config = dotenv_values()

            self._api_key = config["ALPHA_VANTAGE_API_KEY"]

        return self._api_key

    def _update_stock(self, stock: Stock) -> Stock:
        response = requests.get(
            f"https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={stock.moniker}&apikey={self.api_key}"
        )

        if response.status_code != http.HTTPStatus.OK:
            raise Exception(f"{stock.moniker} is not available. Exception: {response.status_code} - {response.text}")

        content = response.json()

        best_matches = content["bestMatches"]

        if len(best_matches) == 0:
            return stock

        best_match = best_matches[0]

        stock.stock_type = best_match["type"].upper()
        stock.name = best_match["name"]
        stock.description = ""
        stock.currency = self.currencies_by_code[best_match["currency"]]

        match stock.stock_type:
            case "EQUITY":
                response = requests.get(
                    f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={stock.moniker}&apikey={self.api_key}"
                )

                if response.status_code != http.HTTPStatus.OK:
                    raise Exception(
                        f"{stock.moniker} is not available. Exception: {response.status_code} - {response.text}"
                        )

                content = response.json()

                stock.description = content["Description"]
                stock.sector_weightings = json.dumps({content["Sector"].lower(): 1.0})

            case "ETF":
                response = requests.get(
                    f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={stock.moniker}&apikey={self.api_key}"
                )

                if response.status_code != http.HTTPStatus.OK:
                    raise Exception(
                        f"{stock.moniker} is not available. Exception: {response.status_code} - {response.text}"
                        )

                content = response.json()

                stock.sector_weightings = json.dumps(
                    {sector["sector"].replace(" ", "_").lower(): sector["weight"] for sector in content["sectors"]}
                )
            # case "FUTURE":
            #     if info["exchange"] == "CMX":
            #         stock.stock_type = "COMMODITY"
            #         stock.sector_weightings = json.dumps({"commodity": 1.0})

        return stock

    @property
    def prices_df(self) -> DataFrame:
        if self._prices_df is None:
            for stock in self.stocks:
                response = requests.get(
                    f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock.moniker}&apikey={self.api_key}"
                )

            self._prices_df = download(self.monikers, keepna=True, rounding=True, **self._prices_download_parameters())

        return self._prices_df

    def _process_prices(self) -> None:
        df = self.prices_df.copy()

        self._processed_prices_df = df["Close"].fillna(0)  # type: ignore[assignment]

    def _compute_updated_price_values(self, moniker: str) -> list[dict[str, Any]]:
        stock_prices_series = self._processed_prices_df[moniker]

        return [
            {
                "stock_id": self.stock_ids_by_moniker[moniker],
                "date": price_date,
                "amount": price_amount,
            }
            for price_date, price_amount in zip(stock_prices_series.index, stock_prices_series.to_list())
        ]
