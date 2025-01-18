from datetime import datetime
from pathlib import Path
from typing import Self, cast

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Connection, Engine, Selectable, func, select
from sqlalchemy.orm import Session

from pyp.database.models import Currency, ExchangeRate


class OutputCommand:
    _df: DataFrame
    output_dir = Path(__file__).parent.parent.parent.parent.parent.parent / "public/js/output"

    def __init__(self, engine: Engine, portfolio_id: int, date: datetime, currency_code: str):
        self.engine = engine
        self.portfolio_id = portfolio_id
        self.date = date
        self.currency_code = currency_code

        self._currency_ids_by_code: dict[str, int] | None = None

    @property
    def _db_query(self) -> Selectable:
        raise NotImplementedError

    @property
    def _df_dtypes(self) -> dict[str, str]:
        raise NotImplementedError

    def _resolve_currency_ids(self) -> Self:
        with Session(self.engine) as session:
            self._currency_ids_by_code = {c.code: c.id for c in session.scalars(select(Currency)).all()}

        return self

    @property
    def _exchange_rates_query(self) -> Selectable:
        base_currency_id = self._currency_ids_by_code[self.currency_code]
        currency_ids = [
            currency_id for currency_id in self._currency_ids_by_code.values() if currency_id != base_currency_id
        ]

        return (
            select(
                ExchangeRate.from_currency_id,
                ExchangeRate.to_currency_id,
                func.strftime("%Y-%m", func.max(ExchangeRate.date)).label("month"),
                ExchangeRate.rate,
            )
            .select_from(ExchangeRate)
            .where(ExchangeRate.to_currency_id == base_currency_id)
            .where(ExchangeRate.from_currency_id.in_(currency_ids))
            .where(ExchangeRate.date <= self.date.strftime("%Y-%m-%d"))
            .group_by(func.strftime("%Y-%m", ExchangeRate.date))
            .order_by(ExchangeRate.date)
        )

    @property
    def _exchange_rates_df(self) -> Self:
        with Session(self.engine) as session:
            df = pd.read_sql(self._exchange_rates_query, cast(Connection, session.bind)).astype(
                dtype={
                    "from_currency_id": "int64",
                    "to_currency_id": "int64",
                    "month": "string",
                    "rate": "float64",
                }
            )

            df["month"] = df["month"].apply(lambda x: datetime.strptime(x, "%Y-%m").strftime("%b-%y"))

        return df

    def _add_exchange_rates(self) -> Self:
        self._df = self._df.merge(
            self._exchange_rates_df,
            how="left",
            left_on=["month", "currency_id"],
            right_on=["month", "from_currency_id"],
        )

        self._df["rate"] = self._df["rate"].fillna(1)

        self._df = self._df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

        return self

    def _prepare_df(self) -> None:
        raise NotImplementedError

    def _write_data_files(self) -> None:
        raise NotImplementedError

    def _show(self) -> None:
        raise NotImplementedError

    def _read_db(self) -> Self:
        with Session(self.engine) as session:
            self._df = pd.read_sql(self._db_query, cast(Connection, session.bind)).astype(dtype=self._df_dtypes)

        return self

    def _compute_share_value(self) -> Self:
        self._df["value"] = self._df["amount"] * self._df["price"]
        self._df = self._df.drop(columns=["price"])

        return self

    def execute(self) -> None:
        self._prepare_df()

        self._write_data_files()
