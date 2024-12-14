import json
from functools import cached_property
from typing import Sequence

from pandas import DataFrame
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from yfinance import Ticker, download

from pyp.database.engine import engine
from pyp.database.models import Currency, Price, Stock

# TODO Test


class IngestStocks:
    @property
    def stocks(self) -> Sequence[Stock]:
        with Session(engine) as session:
            stocks = session.scalars(select(Stock)).all()

        return stocks

    @cached_property
    def stocks_by_moniker(self) -> dict[str, Stock]:
        with Session(engine) as session:
            stocks = session.scalars(select(Stock)).all()

        return {s.moniker: s for s in stocks}

    @cached_property
    def monikers(self) -> list[str]:
        # return [stock.moniker for stock in self.stocks if stock.moniker != "BTCO"]
        return [stock.moniker for stock in self.stocks]

    @cached_property
    def currencies(self) -> dict[str, Currency]:
        with Session(engine) as session:
            currencies = session.scalars(select(Currency)).all()

        return {c.name: c for c in currencies}

    def update_stock_info(self) -> None:
        with Session(engine) as session:
            for stock in self.stocks:
                session.add(stock)

                ticker = Ticker(stock.moniker)
                info = ticker.info

                stock.stock_type = info["quoteType"]
                stock.name = info["longName"]
                stock.description = info["longBusinessSummary"]

                match info["quoteType"]:
                    case "EQUITY":
                        stock.sector_weightings = json.dumps({info["sectorKey"]: 1.0})
                    case "ETF":
                        if ticker.funds_data.sector_weightings:
                            stock.sector_weightings = json.dumps(ticker.funds_data.sector_weightings)
                        else:
                            stock.sector_weightings = json.dumps({info["category"].replace(" ", "_").lower(): 1.0})

                stock.currency = self.currencies[info["currency"]]

                session.commit()

    def update_stock_pricing(self, start_date: str | None, end_date: str | None) -> None:
        download_params = {"period": "1y"}

        if start_date is not None and end_date is not None:
            download_params = {"start": start_date, "end": end_date}

        download_df: DataFrame = download(self.monikers, **download_params)

        adj_close_series = download_df["Adj Close"].dropna()

        with Session(engine) as session:
            for moniker in self.monikers:
                stock_prices_series = adj_close_series[moniker]

                print(stock_prices_series)

                prices = [
                    Price(
                        stock=self.stocks_by_moniker[moniker],
                        date=price_date,
                        amount=price_amount,
                    )
                    for price_date, price_amount in zip(stock_prices_series.index, stock_prices_series.to_list())
                ]

                session.add_all(prices)

            try:
                session.commit()
            except IntegrityError:
                print("Already have prices. Should implement a real UPSERT.")

    def ingest(self, start_date: str | None = None, end_date: str | None = None) -> None:
        self.update_stock_info()
        self.update_stock_pricing(start_date, end_date)
