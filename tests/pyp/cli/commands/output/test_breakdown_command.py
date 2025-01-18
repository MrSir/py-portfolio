import json
from datetime import datetime
from unittest.mock import MagicMock, call

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture
from sqlalchemy import Selectable

from pyp.cli.commands.output.base import OutputCommand
from pyp.cli.commands.output.breakdown import OutputBreakdownCommand
from pyp.cli.protocols import OutputCommandProtocol
from pyp.database.models import Currency


@pytest.fixture
def command(portfolio_id: int, mock_engine: MagicMock) -> OutputBreakdownCommand:
    return OutputBreakdownCommand(mock_engine, portfolio_id, datetime(2024, 12, 3), "USD")


@pytest.fixture
def db_data_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "BTCO", "PLTR", "IYK", "AP-UN.TO"],
            "stock_type": ["EQUITY", "ETF", "EQUITY", "ETF", "ETF"],
            "currency_id": [1, 1, 1, 1, 2],
            "sector_weightings": [
                json.dumps({"technology": 1.0}),
                json.dumps({"digital_assets": 1.0}),
                json.dumps({"technology": 1.0}),
                json.dumps({
                    "realestate": 0.0,
                    "consumer_cyclical": 0.0076,
                    "basic_materials": 0.0201,
                    "consumer_defensive": 0.8869,
                    "technology": 0.0,
                    "communication_services": 0.0,
                    "financial_services": 0.0,
                    "utilities": 0.0,
                    "industrials": 0.00029999999,
                    "energy": 0.0,
                    "healthcare": 0.0851,
                }),
                json.dumps({
                    "realestate": 0.0,
                    "consumer_cyclical": 0.0716,
                    "basic_materials": 0.081099994,
                    "consumer_defensive": 0.0,
                    "technology": 0.1965,
                    "communication_services": 0.038399998,
                    "financial_services": 0.4108,
                    "utilities": 0.0,
                    "industrials": 0.0871,
                    "energy": 0.0996,
                    "healthcare": 0.0149,
                }),
            ],
            "amount": [1.782, 11.000, 6.550, 20.447, 43.240],
            "price": [303.570007, 95.599998, 70.959999, 70.029999, 63.830002],
        }
    ).astype(
        dtype={
            "moniker": "string",
            "stock_type": "string",
            "currency_id": "int64",
            "sector_weightings": "object",
            "amount": "float64",
            "price": "float64",
        }
    )


@pytest.fixture
def share_value_df(db_data_df: DataFrame) -> DataFrame:
    df = db_data_df.copy(deep=True)
    df["value"] = df["amount"] * df["price"]

    return df.drop(columns=["price"])


@pytest.fixture
def exchange_rates_df() -> DataFrame:
    return DataFrame(
        data={
            "from_currency_id": [2],
            "to_currency_id": [1],
            "date": ["2025-01-01"],
            "rate": [0.7872713961],
        }
    ).astype(
        dtype={
            "from_currency_id": "int64",
            "to_currency_id": "int64",
            "date": "string",
            "rate": "float64",
        }
    )


@pytest.fixture
def with_exchange_rates_df(share_value_df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
    df = share_value_df.copy(deep=True)
    df = df.merge(
        exchange_rates_df,
        how="left",
        left_on=["currency_id"],
        right_on=["from_currency_id"],
    )

    df["rate"] = df["rate"].fillna(1)

    df = df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

    return df


@pytest.fixture
def expand_by_sector_df(share_value_df: DataFrame) -> DataFrame:
    df = share_value_df.copy(deep=True)
    df["sector_weightings"] = df["sector_weightings"].str.replace("_", "-")
    df = df.join(DataFrame(share_value_df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))

    return df.drop(columns=["amount", "sector_weightings"])


@pytest.fixture
def percent_by_moniker_df(expand_by_sector_df: DataFrame) -> DataFrame:
    df = expand_by_sector_df.copy(deep=True)
    df["total_value"] = df["value"].sum()
    df["percent"] = df["value"] / df["total_value"]

    return df.drop(columns=["value", "total_value"])


@pytest.fixture
def moniker_breakdown_df(percent_by_moniker_df: DataFrame) -> DataFrame:
    df = percent_by_moniker_df.copy(deep=True)

    return df[["moniker", "percent"]]


def test_initialization(portfolio_id: int, mock_engine: MagicMock) -> None:
    date = datetime(2024, 12, 9)
    currency_code = "USD"
    command = OutputBreakdownCommand(mock_engine, portfolio_id, date, currency_code)

    assert isinstance(command, OutputCommand)
    assert isinstance(command, OutputCommandProtocol)

    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert currency_code == command.currency_code


def test_db_query_property(command: OutputBreakdownCommand) -> None:
    db_query = command._db_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        stocks.stock_type,
        stocks.currency_id,
        stocks.sector_weightings,
        sum(shares.amount) AS amount,
        (SELECT prices.amount
        FROM prices
        WHERE prices.stock_id = stocks.id
        AND prices.date <= :date_1
        ORDER BY prices.date DESC
        LIMIT :param_1) AS price
    FROM shares
    JOIN portfolio_stocks ON portfolio_stocks.id = shares.portfolio_stocks_id
    JOIN stocks ON stocks.id = portfolio_stocks.stock_id
    WHERE shares.purchased_on <= :purchased_on_1
        AND portfolio_stocks.portfolio_id = :portfolio_id_1
    GROUP BY stocks.moniker"""

    expected_query = query.replace("\n        ", " ").replace("\n    ", " ")

    assert expected_query == str(db_query).replace("\n", "")


def test_df_dtypes_property(command: OutputBreakdownCommand) -> None:
    assert {
        "moniker": "string",
        "stock_type": "string",
        "currency_id": "int64",
        "sector_weightings": "object",
        "amount": "float64",
        "price": "float64",
    } == command._df_dtypes


def test_exchange_rates_query_property(command: OutputBreakdownCommand) -> None:
    currencies = [Currency(id=1, code="USD"), Currency(id=2, code="CAD")]
    command._currency_ids_by_code = {c.code: c.id for c in currencies}

    db_query = command._exchange_rates_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        exchange_rates.from_currency_id,
        exchange_rates.to_currency_id,
        max(exchange_rates.date) AS date,
        exchange_rates.rate
    FROM exchange_rates
    WHERE exchange_rates.to_currency_id = :to_currency_id_1
    AND exchange_rates.from_currency_id IN (__[POSTCOMPILE_from_currency_id_1])
    AND exchange_rates.date <= :date_1
    GROUP BY exchange_rates.from_currency_id, exchange_rates.to_currency_id"""

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
    command: OutputBreakdownCommand,
    exchange_rates_df: DataFrame,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.commands.output.breakdown.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=exchange_rates_df)
    mocker.patch("pyp.cli.commands.output.breakdown.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.commands.output.breakdown.OutputBreakdownCommand._exchange_rates_query", mock_property)

    assert exchange_rates_df.equals(command._exchange_rates_df)

    mock_session_class.assert_called_once_with(command.engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_add_exchange_rates(
    command: OutputBreakdownCommand,
    share_value_df: DataFrame,
    exchange_rates_df: DataFrame,
    mocker: MockerFixture,
) -> None:
    command._df = share_value_df

    mock_property = mocker.PropertyMock(return_value=exchange_rates_df)
    mocker.patch("pyp.cli.commands.output.breakdown.OutputBreakdownCommand._exchange_rates_df", mock_property)

    assert command == command._add_exchange_rates()

    df = share_value_df.copy(deep=True)
    df = df.merge(
        exchange_rates_df,
        how="left",
        left_on=["currency_id"],
        right_on=["from_currency_id"],
    )

    df["rate"] = df["rate"].fillna(1)
    df = df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

    assert df.equals(command._df)


def test_convert_to_currency(command: OutputBreakdownCommand, with_exchange_rates_df: DataFrame) -> None:
    command._df = with_exchange_rates_df

    df = with_exchange_rates_df.copy(deep=True)
    df["amount"] = df["amount"] * df["rate"]
    df["value"] = df["value"] * df["rate"]

    df = df.drop(columns=["rate", "date"])

    command._convert_to_currency()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_expand_by_sector(command: OutputBreakdownCommand, share_value_df: DataFrame) -> None:
    command._df = share_value_df

    df = share_value_df.copy(deep=True)
    df["sector_weightings"] = df["sector_weightings"].str.replace("_", "-")

    df = df.join(DataFrame(df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0)).drop(
        columns=["amount", "sector_weightings"]
    )

    assert command == command._expand_by_sector()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_percent_by_moniker(command: OutputBreakdownCommand, expand_by_sector_df: DataFrame) -> None:
    command._df = expand_by_sector_df

    df = expand_by_sector_df.copy(deep=True)
    df["total_value"] = df["value"].sum()
    df["percent"] = df["value"] / df["total_value"]
    df = df.drop(columns=["value", "total_value"])

    assert command == command._calculate_percent_by_moniker()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_moniker_breakdown_df_property(command: OutputBreakdownCommand, percent_by_moniker_df: DataFrame) -> None:
    command._df = percent_by_moniker_df

    df = percent_by_moniker_df[["moniker", "percent"]].copy(deep=True)

    percent_by_moniker_df = command._moniker_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_stock_type_breakdown_df_property(command: OutputBreakdownCommand, percent_by_moniker_df: DataFrame) -> None:
    command._df = percent_by_moniker_df

    df = percent_by_moniker_df[["stock_type", "percent"]].copy(deep=True).groupby("stock_type").sum().reset_index()

    percent_by_moniker_df = command._stock_type_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_sector_breakdown_df_property(command: OutputBreakdownCommand, percent_by_moniker_df: DataFrame) -> None:
    command._df = percent_by_moniker_df

    df = percent_by_moniker_df.copy(deep=True).drop(columns=["moniker", "stock_type"])
    df_minus_percent = df.drop(columns="percent")
    df = df_minus_percent.multiply(df["percent"], axis="index")
    df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

    percent_by_moniker_df = command._sector_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_prepare_df(command: OutputBreakdownCommand, mocker: MockerFixture) -> None:
    mock_rdb = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_read_db", mock_rdb)
    mock_csv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_share_value", mock_csv)
    mock_rci = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_resolve_currency_ids", mock_rci)
    mock_aer = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_add_exchange_rates", mock_aer)
    mock_ctc = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_convert_to_currency", mock_ctc)
    mock_ebs = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_expand_by_sector", mock_ebs)
    mock_cpbm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_calculate_percent_by_moniker", mock_cpbm)

    command._prepare_df()

    mock_rdb.assert_called_once()
    mock_csv.assert_called_once()
    mock_rci.assert_called_once()
    mock_aer.assert_called_once()
    mock_ctc.assert_called_once()
    mock_ebs.assert_called_once()
    mock_cpbm.assert_called_once()


def test_writes_data_files(command: OutputBreakdownCommand, mocker: MockerFixture) -> None:
    mock_moniker_breakdown_df = mocker.MagicMock()
    mock_moniker_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.breakdown.OutputBreakdownCommand._moniker_breakdown_df", mock_moniker_breakdown_df
    )
    mock_stock_type_breakdown_df = mocker.MagicMock()
    mock_stock_type_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.breakdown.OutputBreakdownCommand._stock_type_breakdown_df",
        mock_stock_type_breakdown_df,
    )
    mock_sector_breakdown_df = mocker.MagicMock()
    mock_sector_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.breakdown.OutputBreakdownCommand._sector_breakdown_df", mock_sector_breakdown_df
    )

    mock_open = mocker.MagicMock()
    mock_file = mocker.MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mocker.patch("pyp.cli.commands.output.breakdown.open", mock_open)

    command._write_data_files()
    assert command.output_dir is not None

    mock_moniker_breakdown_df.to_json.assert_called_once_with(orient="records")
    mock_stock_type_breakdown_df.to_json.assert_called_once_with(orient="records")
    mock_sector_breakdown_df.to_json.assert_called_once_with(orient="records")

    mock_open.assert_called_once_with(command.output_dir / "breakdown.js", "w")
    mock_file.write.assert_has_calls([
        call(f"breakdown_by_moniker_data = {mock_moniker_breakdown_df.to_json.return_value}\n"),
        call(f"breakdown_by_stock_type_data = {mock_stock_type_breakdown_df.to_json.return_value}\n"),
        call(f"breakdown_by_sector_data = {mock_sector_breakdown_df.to_json.return_value}\n"),
    ])
