from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture
from sqlalchemy import Selectable

from pyp.cli.commands.output.base import OutputCommand
from pyp.database.models import Currency


@pytest.fixture
def command(portfolio_id: int, mock_engine: MagicMock) -> OutputCommand:
    return OutputCommand(mock_engine, portfolio_id, datetime(2024, 12, 3), "USD")


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


def test_initialization(portfolio_id: int, mock_engine: MagicMock) -> None:
    date = datetime(2024, 12, 9)
    currency_code = "USD"

    command = OutputCommand(mock_engine, portfolio_id, date, currency_code)

    assert mock_engine == command.engine
    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert currency_code == command.currency_code

    assert command._currency_ids_by_code is None

    expected_path = Path(__file__).parent.parent.parent.parent.parent.parent / "public/js/output"
    assert expected_path == command.output_dir


def test_db_query_raises_not_implemented_error(command: OutputCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._db_query


def test_df_dtypes_raises_not_implemented_error(command: OutputCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._df_dtypes


def test_resolve_currency_ids(
    command: OutputCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    currencies = [Currency(id=1, code="USD"), Currency(id=2, code="CAD")]

    mocker.patch("pyp.cli.commands.output.base.Session", mock_session_class)
    mocker.patch("pyp.cli.commands.output.base.select", mock_select)
    mock_session.scalars.return_value.all.return_value = currencies

    command._resolve_currency_ids()

    assert {c.code: c.id for c in currencies} == command._currency_ids_by_code

    mock_session_class.assert_called_once_with(command.engine)
    mock_session.scalars.assert_called_once()
    mock_select.assert_called_once_with(Currency)


def test_exchange_rates_query_property(command: OutputCommand) -> None:
    currencies = [Currency(id=1, code="USD"), Currency(id=2, code="CAD")]
    command._currency_ids_by_code = {c.code: c.id for c in currencies}

    db_query = command._exchange_rates_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        exchange_rates.from_currency_id,
        exchange_rates.to_currency_id,
        strftime(:strftime_1, max(exchange_rates.date)) AS month,
        exchange_rates.rate
    FROM exchange_rates
    WHERE exchange_rates.to_currency_id = :to_currency_id_1
    AND exchange_rates.from_currency_id IN (__[POSTCOMPILE_from_currency_id_1])
    AND exchange_rates.date <= :date_1
    GROUP BY strftime(:strftime_2, exchange_rates.date)
    ORDER BY exchange_rates.date"""

    expected_query = (
        query.replace("(\n            ", "(")
        .replace("\n            ", " ")
        .replace("\n            ", " ")
        .replace("\n        )", ")")
        .replace("\n        ", " ")
        .replace("\n    ", " ")
    )

    assert expected_query == str(db_query).replace("\n", "")


def test_exchange_rates_df(
    command: OutputCommand,
    exchange_rates_df: DataFrame,
    formatted_exchange_rates_df: DataFrame,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.commands.output.base.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=exchange_rates_df)
    mocker.patch("pyp.cli.commands.output.base.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.commands.output.base.OutputCommand._exchange_rates_query", mock_property)

    assert formatted_exchange_rates_df.equals(command._exchange_rates_df)

    mock_session_class.assert_called_once_with(command.engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_add_exchange_rates(
    command: OutputCommand,
    market_value_df: DataFrame,
    formatted_exchange_rates_df: DataFrame,
    mocker: MockerFixture,
) -> None:
    command._df = market_value_df

    mock_property = mocker.PropertyMock(return_value=formatted_exchange_rates_df)
    mocker.patch("pyp.cli.commands.output.base.OutputCommand._exchange_rates_df", mock_property)

    assert command == command._add_exchange_rates()

    df = market_value_df.copy(deep=True)
    df = df.merge(
        formatted_exchange_rates_df,
        how="left",
        left_on=["month", "currency_id"],
        right_on=["month", "from_currency_id"],
    )

    df["rate"] = df["rate"].fillna(1)
    df = df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

    assert df.equals(command._df)


def test_read_db(
    command: OutputCommand,
    df: DataFrame,
    df_dtypes,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.commands.output.base.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=df)
    mocker.patch("pyp.cli.commands.output.base.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.commands.output.base.OutputCommand._db_query", mock_property)

    mock_dtypes_property = mocker.PropertyMock(return_value=df_dtypes)
    mocker.patch("pyp.cli.commands.output.base.OutputCommand._df_dtypes", mock_dtypes_property)

    assert command == command._read_db()

    mock_session_class.assert_called_once_with(command.engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_prepare_df_raises_not_implemented_error(command: OutputCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._prepare_df()


def test_write_data_files_raises_not_implemented_error(command: OutputCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._write_data_files()


def test_show_raises_not_implemented_error(command: OutputCommand) -> None:
    with pytest.raises(NotImplementedError):
        command._show()


def test_compute_share_value(command: OutputCommand, df: DataFrame) -> None:
    command._df = df

    expected_df = df.copy(deep=True)

    expected_df["value"] = expected_df["amount"] * expected_df["price"]
    expected_df = expected_df.drop(columns=["price"])

    assert command == command._compute_share_value()

    assert isinstance(command._df, DataFrame)
    assert expected_df.equals(command._df)


def test_execute_writes_data_files(command: OutputCommand, mocker: MockerFixture) -> None:
    mock_pdf = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_df", mock_pdf)

    mock_write_data_files = mocker.MagicMock()
    mocker.patch.object(command, "_write_data_files", mock_write_data_files)

    command.execute()

    mock_pdf.assert_called_once()
    mock_write_data_files.assert_called_once()
