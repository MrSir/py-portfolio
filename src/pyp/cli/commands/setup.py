import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import Engine, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from pyp.database.models import Base, Currency, ExchangeRate


class SetupCommand:
    data_path: Path = Path(__file__).parent.parent.parent.parent.parent / "data"
    _currency_id_by_code: dict[str, int]

    def __init__(self, engine: Engine, seed: bool = False):
        self.engine = engine
        self.seed = seed

    def _create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def _read_currencies(self) -> list[dict]:
        with open(self.data_path / "currencies.json", "r") as file:
            default_currencies = json.loads(file.read())

        return default_currencies

    def _seed_currencies(self) -> None:
        with Session(self.engine) as session:
            statement = insert(Currency).values(self._read_currencies())
            statement = statement.on_conflict_do_nothing(index_elements=["code"])

            session.execute(statement)
            session.commit()

    def _resolve_currency_ids(self) -> None:
        with Session(self.engine) as session:
            self._currency_id_by_code = {c.code: c.id for c in session.scalars(select(Currency)).all()}

    def _read_exchange_rates_for_code(self, code: str) -> list[dict]:
        exchange_rates = []

        currency_exchange_rates_path = self.data_path / "exchange_rates" / code

        for file_path in currency_exchange_rates_path.glob("*.json"):
            with open(file_path, "r") as file:
                contents = json.loads(file.read())
                exchange_rates += [
                    {
                        "from_currency_id": self._currency_id_by_code[code],
                        "to_currency_id": self._currency_id_by_code[to_code],
                        "date": datetime.strptime(exchange_rate_date, "%Y-%m-%d"),
                        "rate": rate,
                    }
                    for exchange_rate_date, rates in contents["data"].items()
                    for to_code, rate in rates.items()
                ]

        return exchange_rates

    def _seed_exchange_rates(self) -> None:
        with Session(self.engine) as session:
            for code in self._currency_id_by_code.keys():
                statement = insert(ExchangeRate).values(self._read_exchange_rates_for_code(code))
                statement = statement.on_conflict_do_nothing(
                    index_elements=["from_currency_id", "to_currency_id", "date"]
                )

                session.execute(statement)
                session.commit()

    def execute(self) -> None:
        self._create_schema()

        if self.seed:
            self._seed_currencies()
            self._resolve_currency_ids()
            self._seed_exchange_rates()
