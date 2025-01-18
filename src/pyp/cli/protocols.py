from typing import Protocol, runtime_checkable

from sqlalchemy import Selectable


@runtime_checkable
class CommandProtocol(Protocol):
    def execute(self) -> None: ...


@runtime_checkable
class OutputCommandProtocol(CommandProtocol, Protocol):
    @property
    def _db_query(self) -> Selectable: ...

    @property
    def _df_dtypes(self) -> dict[str, str]: ...

    def _prepare_df(self) -> None: ...

    def _write_data_files(self) -> None: ...
