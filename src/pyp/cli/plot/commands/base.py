from datetime import datetime
from pathlib import Path
from typing import Self, cast

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Connection, Selectable
from sqlalchemy.orm import Session

from pyp.database.engine import engine


class PlotCommand:
    def __init__(self, portfolio_id: int, date: datetime, output_dir: Path | None = None):
        self.portfolio_id = portfolio_id
        self.date = date
        self.output_dir = output_dir

        self._df: DataFrame | None = None

    @property
    def _db_query(self) -> Selectable:
        raise NotImplementedError

    @property
    def _df_dtypes(self) -> dict[str, str]:
        raise NotImplementedError

    def _prepare_df(self) -> None:
        raise NotImplementedError

    def _write_json_files(self) -> None:
        raise NotImplementedError

    def _show(self) -> None:
        raise NotImplementedError

    def _read_db(self) -> Self:
        with Session(engine) as session:
            self._df = pd.read_sql(self._db_query, cast(Connection, session.bind)).astype(dtype=self._df_dtypes)

        return self

    def _compute_share_value(self) -> Self:
        if self._df is not None:
            self._df["value"] = self._df["amount"] * self._df["price"]
            self._df = self._df.drop(columns=["price"])

        return self

    def execute(self) -> None:
        self._prepare_df()

        if self.output_dir is not None:
            self._write_json_files()

            return

        self._show()
