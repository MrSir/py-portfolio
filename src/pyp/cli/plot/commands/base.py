from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import cast

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

    @property
    def db_query(self) -> Selectable:
        raise NotImplementedError

    @property
    def db_data_df_dtypes(self) -> dict[str, str]:
        raise NotImplementedError

    def write_json_files(self) -> None:
        raise NotImplementedError

    def show(self) -> None:
        raise NotImplementedError

    @cached_property
    def db_data_df(self) -> DataFrame:
        with Session(engine) as session:
            db_data_df = pd.read_sql(self.db_query, cast(Connection, session.bind)).astype(dtype=self.db_data_df_dtypes)

        return db_data_df

    @property
    def share_value_df(self) -> DataFrame:
        df = self.db_data_df.copy(deep=True)
        df["value"] = df["amount"] * df["price"]

        return df.drop(columns=["price"])

    def plot(self) -> None:
        if self.output_dir is not None:
            self.write_json_files()

            return

        self.show()
