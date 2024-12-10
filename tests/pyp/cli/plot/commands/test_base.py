from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable

from pyp.cli.plot.commands.base import PlotCommand
from pyp.database.engine import engine


@pytest.fixture
def command(portfolio_id: int) -> PlotCommand:
    return PlotCommand(portfolio_id, datetime(2024, 12, 3), output_dir=Path(__file__).parent)


@pytest.fixture
def db_data_df_dtypes() -> dict[str, str]:
    return {
        "moniker": "string",
        "amount": "float64",
        "price": "float64",
    }


@pytest.fixture
def db_data_df(db_data_df_dtypes: dict[str, str]) -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "BTCO", "PLTR", "IYK", "RDVY"],
            "amount": [1.782, 11.000, 6.550, 20.447, 43.240],
            "price": [303.570007, 95.599998, 70.959999, 70.029999, 63.830002],
        }
    ).astype(dtype=db_data_df_dtypes)


def test_initialization(portfolio_id: int) -> None:
    output_dir = Path(__file__).parent
    date = datetime(2024, 12, 9)

    command = PlotCommand(portfolio_id, date, output_dir=output_dir)

    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert output_dir == command.output_dir


def test_db_query_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command.db_query


def test_db_data_df_dtypes_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command.db_data_df_dtypes


def test_write_json_files_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command.write_json_files()


def test_show_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command.show()


def test_db_data_df_property(
    command: PlotCommand,
    db_data_df: DataFrame,
    db_data_df_dtypes: dict[str, str],
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockFixture,
) -> None:
    mocker.patch("pyp.cli.plot.commands.base.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.base.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand.db_query", mock_property)

    mock_dtypes_property = mocker.PropertyMock(return_value=db_data_df_dtypes)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand.db_data_df_dtypes", mock_dtypes_property)

    actual_df = command.db_data_df

    assert isinstance(actual_df, DataFrame)
    assert db_data_df.equals(actual_df)

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_db_data_df_property_caches(
    command: PlotCommand,
    db_data_df: DataFrame,
    db_data_df_dtypes: dict[str, str],
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockFixture,
) -> None:
    mocker.patch("pyp.cli.plot.commands.base.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.base.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand.db_query", mock_property)

    mock_dtypes_property = mocker.PropertyMock(return_value=db_data_df_dtypes)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand.db_data_df_dtypes", mock_dtypes_property)

    assert isinstance(command.db_data_df, DataFrame), "First time it computes"
    assert isinstance(command.db_data_df, DataFrame), "Second time it caches"

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_share_value_df_property(command: PlotCommand, db_data_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand.db_data_df", mock_property)

    df = db_data_df.copy(deep=True)
    df["value"] = df["amount"] * df["price"]
    df = df.drop(columns=["price"])

    share_value_df = command.share_value_df

    assert isinstance(share_value_df, DataFrame)
    assert df.equals(share_value_df)


def test_plot_writes_json_files_when_output_dir_is_provided(command: PlotCommand, mocker: MockFixture) -> None:
    mock_write_json_files = mocker.MagicMock()
    mocker.patch.object(command, "write_json_files", mock_write_json_files)

    mock_show = mocker.MagicMock()
    mocker.patch.object(command, "show", mock_show)

    command.plot()

    mock_write_json_files.assert_called_once()
    mock_show.assert_not_called()


def test_plot_shows_when_output_dir_is_none(command: PlotCommand, mocker: MockFixture) -> None:
    mock_write_json_files = mocker.MagicMock()
    mocker.patch.object(command, "write_json_files", mock_write_json_files)

    mock_show = mocker.MagicMock()
    mocker.patch.object(command, "show", mock_show)

    command.output_dir = None

    command.plot()

    mock_write_json_files.assert_not_called()
    mock_show.assert_called_once()
