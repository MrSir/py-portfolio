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
def df_dtypes() -> dict[str, str]:
    return {
        "moniker": "string",
        "amount": "float64",
        "price": "float64",
    }


@pytest.fixture
def df(df_dtypes) -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "BTCO", "PLTR", "IYK", "RDVY"],
            "amount": [1.782, 11.000, 6.550, 20.447, 43.240],
            "price": [303.570007, 95.599998, 70.959999, 70.029999, 63.830002],
        }
    ).astype(dtype=df_dtypes)


def test_initialization(portfolio_id: int) -> None:
    output_dir = Path(__file__).parent
    date = datetime(2024, 12, 9)

    command = PlotCommand(portfolio_id, date, output_dir=output_dir)

    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert output_dir == command.output_dir
    assert command._df is None


def test_db_query_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._db_query


def test_df_dtypes_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._df_dtypes


def test_read_db(
    command: PlotCommand,
    df: DataFrame,
    df_dtypes,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockFixture,
) -> None:
    mocker.patch("pyp.cli.plot.commands.base.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=df)
    mocker.patch("pyp.cli.plot.commands.base.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand._db_query", mock_property)

    mock_dtypes_property = mocker.PropertyMock(return_value=df_dtypes)
    mocker.patch("pyp.cli.plot.commands.base.PlotCommand._df_dtypes", mock_dtypes_property)

    assert command == command._read_db()

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_prepare_df_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._prepare_df()


def test_write_json_files_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._write_json_files()


def test_show_raises_not_implemented_error(command: PlotCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._show()


def test_compute_share_value(command: PlotCommand, df: DataFrame) -> None:
    command._df = df

    df["value"] = df["amount"] * df["price"]
    df = df.drop(columns=["price"])

    assert command == command._compute_share_value()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_plot_writes_json_files_when_output_dir_is_provided(command: PlotCommand, mocker: MockFixture) -> None:
    mock_pdf = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_df", mock_pdf)

    mock_write_json_files = mocker.MagicMock()
    mocker.patch.object(command, "_write_json_files", mock_write_json_files)

    mock_show = mocker.MagicMock()
    mocker.patch.object(command, "_show", mock_show)

    command.execute()

    mock_pdf.assert_called_once()
    mock_write_json_files.assert_called_once()
    mock_show.assert_not_called()


def test_plot_shows_when_output_dir_is_none(command: PlotCommand, mocker: MockFixture) -> None:
    mock_pdf = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_df", mock_pdf)

    mock_write_json_files = mocker.MagicMock()
    mocker.patch.object(command, "_write_json_files", mock_write_json_files)

    mock_show = mocker.MagicMock()
    mocker.patch.object(command, "_show", mock_show)

    command.output_dir = None

    command.execute()

    mock_pdf.assert_called_once()
    mock_write_json_files.assert_not_called()
    mock_show.assert_called_once()
