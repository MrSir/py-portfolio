from typing import Self

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from pyp.database.models import Base, Currency


class SetupCommand:
    def __init__(self, engine: Engine, seed: bool = False):
        self.engine = engine
        self.seed = seed

    def _create_schema(self) -> Self:
        Base.metadata.create_all(self.engine)

        return self

    @property
    def _default_currencies(self) -> list[Currency]:
        currencies = [
            {"name": "USD", "full_name": "United States Dollar"},
            {"name": "CAD", "full_name": "Canadian Dollar"},
        ]

        return [Currency(**c) for c in currencies]

    def _seed_db(self) -> Self:
        with Session(self.engine) as session:
            for c in self._default_currencies:
                session.add(c)

            session.commit()

        return self

    def execute(self) -> None:
        self._create_schema()

        if self.seed:
            self._seed_db()
