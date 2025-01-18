from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from pyp.database.models import Currency


class IngestBaseCommand:
    _currencies_by_code: dict[str, Currency]

    def __init__(self, engine: Engine):
        self.engine = engine

    def _resolve_currencies(self) -> None:
        with Session(self.engine) as session:
            self._currencies_by_code = {c.code: c for c in session.scalars(select(Currency)).all()}
