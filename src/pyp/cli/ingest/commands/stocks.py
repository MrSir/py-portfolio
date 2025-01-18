import json
from datetime import datetime
from typing import Sequence

from pandas import DataFrame
from sqlalchemy import Engine, Insert, Select, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session
from yfinance import Ticker, download

from pyp.cli.ingest.commands.base import IngestBaseCommand
from pyp.database.models import Price, Stock


class IngestStocksCommand(IngestBaseCommand):
    _stocks: Sequence[Stock]
    _stock_ids_by_moniker: dict[str, int]
    _monikers: list[str]
    _prices_df: DataFrame
    _close_prices_df: DataFrame

    def __init__(
        self,
        engine: Engine,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        monikers: list[str] | None = None,
        exclude_monikers: list[str] | None = None,
    ):
        super().__init__(engine)

        self.start_date = start_date
        self.end_date = end_date
        self.monikers = monikers
        self.exclude_monikers = exclude_monikers

    def _prepare_stocks_statement(self) -> Select:
        statement = select(Stock)

        if self.monikers is not None:
            statement = statement.where(Stock.moniker.in_(self.monikers))

        if self.exclude_monikers is not None:
            statement = statement.where(Stock.moniker.notin_(self.exclude_monikers))

        return statement

    def _resolve_stocks(self) -> None:
        with Session(self.engine) as session:
            self._stocks = session.scalars(self._prepare_stocks_statement()).all()
            self._stock_ids_by_moniker = {s.moniker: s.id for s in self._stocks}
            self._monikers = list(self._stock_ids_by_moniker.keys())

    def _prepare_updated_stock(self, stock: Stock) -> None:
        ticker = Ticker(stock.moniker)
        info = ticker.info

        stock.stock_type = info["quoteType"]
        stock.name = info["longName"]
        stock.description = info["longBusinessSummary"]
        stock.currency = self._currencies_by_code[info["currency"]]

        match info["quoteType"]:
            case "EQUITY":
                stock.sector_weightings = json.dumps({info["sectorKey"]: 1.0})
            case "ETF":
                if ticker.funds_data.sector_weightings:
                    stock.sector_weightings = json.dumps(ticker.funds_data.sector_weightings)
                else:
                    stock.sector_weightings = json.dumps({info["category"].replace(" ", "_").lower(): 1.0})

    def _update_stock_info(self) -> None:
        with Session(self.engine) as session:
            for stock in self._stocks:
                self._prepare_updated_stock(stock)
                session.add(stock)
                session.commit()

    def _prices_download_parameters(self) -> dict:
        download_params = {"period": "1y"}

        if self.start_date is not None and self.end_date is not None:
            download_params = {"start": self.start_date.strftime("%Y-%m-%d"), "end": self.end_date.strftime("%Y-%m-%d")}

        return download_params

    def _download_prices_df(self) -> None:
        self._prices_df = download(self._monikers, keepna=True, rounding=True, **self._prices_download_parameters())

    def _extract_close_prices(self) -> None:
        self._close_prices_df = self._prices_df["Close"].fillna(0)  # type: ignore[assignment]

    def _prepare_price_upsert_statement(self, moniker: str) -> Insert:
        stock_prices_series = self._close_prices_df[moniker]

        values = [
            {
                "stock_id": self._stock_ids_by_moniker[moniker],
                "date": price_date,
                "amount": price_amount,
            }
            for price_date, price_amount in zip(stock_prices_series.index, stock_prices_series.to_list())
        ]

        statement = insert(Price).values(values)

        return statement.on_conflict_do_update(
            index_elements=["stock_id", "date"],
            set_={"amount": statement.excluded.amount},
        )

    def _update_stock_pricing(self) -> None:
        with Session(self.engine) as session:
            for moniker in self._monikers:
                statement = self._prepare_price_upsert_statement(moniker)

                session.execute(statement)
                session.commit()

    def execute(self) -> None:
        self._resolve_stocks()
        self._resolve_currencies()

        self._update_stock_info()

        self._download_prices_df()
        self._extract_close_prices()
        self._update_stock_pricing()
