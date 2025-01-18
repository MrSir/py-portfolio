from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pyp.database.models import Currency


class AddCurrencyCommand:
    def __init__(self, engine: Engine, code: str, name: str):
        self.engine = engine
        self.code = code
        self.name = name

        self._currency: Currency | None = None

    def _prepare_currency(self) -> None:
        self._currency = Currency(code=self.code)

    def execute(self) -> None:
        self._prepare_currency()

        with Session(self.engine) as session:
            session.add(self._currency)

            try:
                session.commit()
            except IntegrityError:
                print(f"The currency '{self.code}' ({self.name}) already exists.")
