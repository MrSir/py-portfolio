import json
from datetime import datetime
from typing import Any, Sequence

from pandas import DataFrame
from yfinance import Ticker, download

from pyp.database.models import Currency, Stock
from pyp.services.finance_data.base import FinanceDataService


class YahooFinanceDataService(FinanceDataService):
    def _update_stock(self, stock: Stock) -> Stock:
        ticker = Ticker(stock.moniker)
        info = ticker.info

        stock.stock_type = info["quoteType"]
        stock.name = info["longName"] if "longName" in info else info["shortName"]
        stock.description = info["longBusinessSummary"] if "longBusinessSummary" in info else None
        stock.currency = self.currencies_by_code[info["currency"]]

        match info["quoteType"]:
            case "EQUITY":
                stock.sector_weightings = json.dumps({info["sectorKey"]: 1.0})
            case "ETF":
                if ticker.funds_data.sector_weightings:
                    stock.sector_weightings = json.dumps(ticker.funds_data.sector_weightings)
                else:
                    stock.sector_weightings = json.dumps({info["category"].replace(" ", "_").lower(): 1.0})
            case "FUTURE":
                if info["exchange"] == "CMX":
                    stock.stock_type = "COMMODITY"
                    stock.sector_weightings = json.dumps({"commodity": 1.0})

        return stock

    def _prices_download_parameters(self) -> dict:
        download_params = {"period": "1y"}

        if self.start_date is not None and self.end_date is not None:
            download_params = {"start": self.start_date.strftime("%Y-%m-%d"), "end": self.end_date.strftime("%Y-%m-%d")}

        return download_params

    @property
    def prices_df(self) -> DataFrame:
        if self._prices_df is None:
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

