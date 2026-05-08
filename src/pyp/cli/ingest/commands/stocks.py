
from datetime import datetime
from typing import Sequence

from sqlalchemy import Engine, Select, select
from sqlalchemy.orm import Session

from pyp.database.models import Currency, Stock
from pyp.services.finance_data.base import FinanceDataService
from pyp.services.finance_data.yfinance import YahooFinanceDataService


class IngestStocksCommand:
    def __init__(
        self,
        engine: Engine,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        monikers: list[str] | None = None,
        exclude_monikers: list[str] | None = None,
        provider: str = "yfinance",
    ):
        self.engine = engine

        self.start_date = start_date
        self.end_date = end_date
        self.monikers = monikers
        self.exclude_monikers = exclude_monikers
        self.provider = provider

        self._currencies_by_code: dict[str, Currency] | None = None
        self._stocks: Sequence[Stock] | None = None

        self._finance_data_service: FinanceDataService | None = None

    @property
    def currencies_by_code(self) -> dict[str, Currency]:
        if self._currencies_by_code is None:
            with Session(self.engine) as session:
                self._currencies_by_code = {c.code: c for c in session.scalars(select(Currency)).all()}

        return self._currencies_by_code

    def _prepare_stocks_statement(self) -> Select:
        statement = select(Stock)

        if self.monikers is not None:
            statement = statement.where(Stock.moniker.in_(self.monikers))

        if self.exclude_monikers is not None:
            statement = statement.where(Stock.moniker.notin_(self.exclude_monikers))

        return statement

    @property
    def stocks(self) -> Sequence[Stock]:
        if self._stocks is None:
            with Session(self.engine) as session:
                self._stocks = session.scalars(self._prepare_stocks_statement()).all()

        return self._stocks


    @property
    def finance_data_service(self) -> FinanceDataService:
        if self._finance_data_service is None:
            match self.provider:
                case "yfinance":
                    self._finance_data_service = YahooFinanceDataService(
                        self.stocks,
                        self.currencies_by_code,
                        self.start_date,
                        self.end_date,
                    )
                case "alpha_vantage":
                    self._finance_data_service = YahooFinanceDataService(
                        self.stocks,
                        self.currencies_by_code,
                        self.start_date,
                        self.end_date,
                    )

        return self._finance_data_service

    def _update_stocks(self) -> None:
        with Session(self.engine) as session:
            for stock in self.finance_data_service.update_stocks():
                session.add(stock)
                session.commit()

    def _update_prices(self) -> None:
        with Session(self.engine) as session:
            for statement in self.finance_data_service.update_prices():
                session.execute(statement)
                session.commit()

    # TODO Refactor to separate commands
    # - one that igests prices
    # - one that ingests sector details
    # - one that pulls meta data
    def execute(self) -> None:
        self._update_stocks()
        self._update_prices()
