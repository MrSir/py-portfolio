from typing import Protocol, runtime_checkable

from sqlalchemy import Selectable


@runtime_checkable
class PlotCommandProtocol(Protocol):
    @property
    def _db_query(self) -> Selectable: ...

    @property
    def _df_dtypes(self) -> dict[str, str]: ...

    def _prepare_df(self) -> None: ...

    def _write_json_files(self) -> None: ...

    def _show(self) -> None: ...
