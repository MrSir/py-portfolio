from datetime import datetime
from typing import Any, Sequence

from pandas import DataFrame
from sqlalchemy.dialects.sqlite import Insert, insert

from pyp.database.models import Currency, Price, Stock


class FinanceDataService:
    def __init__(
        self,
        stocks: Sequence[Stock],
        currencies_by_code: dict[str, Currency],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.stocks = stocks
        self.currencies_by_code = currencies_by_code
        self.start_date = start_date
        self.end_date = end_date

        self._stock_ids_by_moniker: dict[str, int] | None = None
        self._monikers: list[str] | None = None

        self._prices_df: DataFrame | None = None
        self._processed_prices_df: DataFrame | None = None

    @property
    def stock_ids_by_moniker(self) -> dict[str, int]:
        if self._stock_ids_by_moniker is None:
            self._stock_ids_by_moniker = {s.moniker: s.id for s in self.stocks}

        return self._stock_ids_by_moniker

    @property
    def monikers(self) -> list[str]:
        if self._monikers is None:
            self._monikers = list(self.stock_ids_by_moniker.keys())

        return self._monikers

    def _update_stock(self, stock: Stock) -> Stock: ...

    def update_stocks(self) -> list[Stock]:
        return [self._update_stock(stock) for stock in self.stocks]

    def _process_prices(self) -> None: ...

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

    def _prepare_price_upsert_statement(self, moniker: str) -> Insert:
        statement = insert(Price).values(self._compute_updated_price_values(moniker))

        return statement.on_conflict_do_update(
            index_elements=["stock_id", "date"],
            set_={"amount": statement.excluded.amount},
        )

    def update_prices(self) -> list[Insert]:
        self._process_prices()

        return [self._prepare_price_upsert_statement(moniker) for moniker in self.monikers]
