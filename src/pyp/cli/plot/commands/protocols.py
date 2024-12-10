from typing import Protocol, runtime_checkable

from sqlalchemy import Selectable


@runtime_checkable
class PlotCommandProtocol(Protocol):
    @property
    def db_query(self) -> Selectable: ...

    @property
    def db_data_df_dtypes(self) -> dict[str, str]: ...

    def write_json_files(self) -> None: ...

    def show(self) -> None: ...
