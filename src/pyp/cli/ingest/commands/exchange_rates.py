from datetime import datetime, timedelta

import freecurrencyapi
from sqlalchemy import Engine, Insert
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from pyp.cli.ingest.commands.base import IngestBaseCommand
from pyp.database.models import ExchangeRate


class IngestExchangeRatesCommand(IngestBaseCommand):
    def __init__(self, engine: Engine, start_date: datetime, end_date: datetime, api_key: str):
        super().__init__(engine)

        self.start_date = start_date
        self.end_date = end_date
        self.api_key = api_key

        self._client: freecurrencyapi.Client | None = None
        self._currency_pairs: dict[str, list[str]] = dict()
        self._exchange_rates_values: list[dict] = []

    @property
    def client(self) -> freecurrencyapi.Client:
        if self._client is None:
            self._client = freecurrencyapi.Client(self.api_key)

        return self._client

    def _compute_currency_pairs(self) -> None:
        self._currency_pairs = {
            code: [k for k in self._currencies_by_code.keys() if k != code] for code in self._currencies_by_code.keys()
        }

    def _download_exchange_rates_for(self, date: datetime, code: str, currencies: list[str]) -> list[dict]:
        date_str = date.strftime("%Y-%m-%d")
        data = self.client.historical(date_str, base_currency=code, currencies=currencies)

        exchange_rates_values = []

        for currency_code in currencies:
            exchange_rates_values.append({
                "from_currency_id": self._currencies_by_code[code].id,
                "to_currency_id": self._currencies_by_code[currency_code].id,
                "date": date,
                "rate": data["data"][date_str][currency_code],
            })

        return exchange_rates_values

    def _download_exchange_rates(self) -> None:
        current_date = self.start_date

        while current_date <= self.end_date:
            for code, currencies in self._currency_pairs.items():
                self._exchange_rates_values += self._download_exchange_rates_for(current_date, code, currencies)

            current_date += timedelta(days=1)

    def _prepare_exchange_rates_upsert_statement(self) -> Insert:
        statement = insert(ExchangeRate).values(self._exchange_rates_values)

        return statement.on_conflict_do_update(
            index_elements=["from_currency_id", "to_currency_id", "date"],
            set_={"rate": statement.excluded.rate},
        )

    def _update_exchange_rates(self) -> None:
        with Session(self.engine) as session:
            session.execute(self._prepare_exchange_rates_upsert_statement())
            session.commit()

    def execute(self) -> None:
        self._resolve_currencies()
        self._compute_currency_pairs()

        self._download_exchange_rates()
        self._update_exchange_rates()
