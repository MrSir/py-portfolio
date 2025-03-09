import json
from datetime import datetime
from unittest.mock import MagicMock, call

import numpy as np
import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from pyp.cli.commands.output.growth import OutputGrowthCommand
from pyp.cli.commands.output.summary import OutputSummaryCommand
from pyp.cli.protocols import OutputCommandProtocol


@pytest.fixture
def added_monthly_market_prices_df(monthly_prices_df: DataFrame, invested_df: DataFrame) -> DataFrame:
    df = monthly_prices_df.merge(
        invested_df,
        how="left",
        left_on=["moniker", "month", "stock_type"],
        right_on=["moniker", "month", "stock_type"],
    )

    df[["amount", "invested"]] = df[["amount", "invested"]].fillna(0, axis="columns")
    df["amount"] = df.groupby("moniker", sort=False)["amount"].cumsum()

    return df


@pytest.fixture
def computed_market_value_df(added_monthly_market_prices_df: DataFrame) -> DataFrame:
    df = added_monthly_market_prices_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]

    return df


@pytest.fixture
def with_exchange_rates_df(computed_market_value_df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
    df = computed_market_value_df.copy(deep=True)
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
def summed_up_by_moniker_df(with_exchange_rates_df: DataFrame) -> DataFrame:
    df = (
        with_exchange_rates_df.copy(deep=True)
        .groupby(["moniker"], sort=False)
        .agg({
            "stock_type": "first",
            "invested": "sum",
            "amount": "last",
            "market_price": "last",
            "value": "last",
        })
        .reset_index()
    )

    return df


@pytest.fixture
def computed_average_price_df(summed_up_by_moniker_df: DataFrame) -> DataFrame:
    df = summed_up_by_moniker_df.copy(deep=True)
    df["average_price"] = (df["invested"] / df["amount"]).fillna(0)

    return df


@pytest.fixture
def command(portfolio_id: int, mock_engine: MagicMock) -> OutputSummaryCommand:
    return OutputSummaryCommand(mock_engine, "MrSir", "My Portfolio", portfolio_id, datetime(2024, 12, 3), "USD")


def test_initialization(portfolio_id: int, mock_engine: MagicMock) -> None:
    username = "MrSir"
    portfolio_name = "My Portfolio"
    date = datetime(2024, 12, 9)
    currency_code = "USD"

    command = OutputSummaryCommand(mock_engine, username, portfolio_name, portfolio_id, date, currency_code)

    assert isinstance(command, OutputGrowthCommand)
    assert isinstance(command, OutputCommandProtocol)
    assert username == command.username
    assert portfolio_name == command.portfolio_name

    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert currency_code == command.currency_code


def test_compute_market_value(command: OutputSummaryCommand, added_monthly_market_prices_df: DataFrame) -> None:
    command._df = added_monthly_market_prices_df

    df = added_monthly_market_prices_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]

    assert command == command._compute_market_value()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_convert_to_currency(command: OutputSummaryCommand, with_exchange_rates_df: DataFrame) -> None:
    command._df = with_exchange_rates_df

    df = with_exchange_rates_df.copy(deep=True)
    df["market_price"] = df["market_price"] * df["rate"]
    df["invested"] = df["invested"] * df["rate"]
    df["value"] = df["value"] * df["rate"]

    df = df.drop(columns=["rate"])

    command._convert_to_currency()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_sum_up_by_moniker(command: OutputSummaryCommand, computed_market_value_df: DataFrame) -> None:
    command._df = computed_market_value_df

    df = (
        computed_market_value_df.copy(deep=True)
        .groupby(["moniker"], sort=False)
        .agg({
            "stock_type": "first",
            "invested": "sum",
            "amount": "last",
            "market_price": "last",
            "value": "last",
        })
        .reset_index()
    )

    assert command == command._sum_up_by_moniker()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_average_price(command: OutputSummaryCommand, summed_up_by_moniker_df: DataFrame) -> None:
    command._df = summed_up_by_moniker_df

    df = summed_up_by_moniker_df.copy(deep=True)
    df["average_price"] = (df["invested"] / df["amount"]).replace([-np.inf], np.nan).fillna(0)

    assert command == command._compute_average_price()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_invested_value(command: OutputSummaryCommand, computed_average_price_df: DataFrame) -> None:
    command._df = computed_average_price_df

    expected_value = computed_average_price_df["invested"].sum()

    assert expected_value == command._invested_value()


def test_market_value(command: OutputSummaryCommand, computed_average_price_df: DataFrame) -> None:
    command._df = computed_average_price_df

    expected_value = computed_average_price_df["value"].sum()

    assert expected_value == command._market_value()


def test_number_of_equities(command: OutputSummaryCommand, computed_average_price_df: DataFrame) -> None:
    command._df = computed_average_price_df

    expected_value = len(computed_average_price_df[computed_average_price_df["stock_type"] == "EQUITY"])

    assert expected_value == command._number_of_equities()


def test_number_of_etfs(command: OutputSummaryCommand, computed_average_price_df: DataFrame) -> None:
    command._df = computed_average_price_df

    expected_value = len(computed_average_price_df[computed_average_price_df["stock_type"] == "ETF"])

    assert expected_value == command._number_of_etfs()


def test_prepare_df(command: OutputGrowthCommand, mocker: MockerFixture) -> None:
    mock_rdb = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_read_db", mock_rdb)
    mock_csv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_share_value", mock_csv)
    mock_rvti = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_rename_value_to_invested", mock_rvti)
    mock_ammp = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_add_monthly_market_prices", mock_ammp)
    mock_cmv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_market_value", mock_cmv)
    mock_rci = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_resolve_currency_ids", mock_rci)
    mock_aer = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_add_exchange_rates", mock_aer)
    mock_ctc = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_convert_to_currency", mock_ctc)
    mock_subm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_sum_up_by_moniker", mock_subm)
    mock_cap = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_average_price", mock_cap)

    command._prepare_df()

    mock_rdb.assert_called_once()
    mock_csv.assert_called_once()
    mock_rvti.assert_called_once()
    mock_ammp.assert_called_once()
    mock_cmv.assert_called_once()
    mock_subm.assert_called_once()
    mock_cap.assert_called_once()


def test_writes_data_files(command: OutputSummaryCommand, mocker: MockerFixture) -> None:
    mock_df = mocker.MagicMock()
    mock_df.drop = mocker.MagicMock(return_value=mock_df)
    mock_df.to_json = mocker.MagicMock()
    command._df = mock_df

    invested_value = 123.457
    mock_invested_value = mocker.MagicMock(return_value=invested_value)
    mocker.patch.object(command, "_invested_value", mock_invested_value)
    market_value = 678.907
    mock_market_value = mocker.MagicMock(return_value=market_value)
    mocker.patch.object(command, "_market_value", mock_market_value)
    number_of_equities = 6
    mock_number_of_equities = mocker.MagicMock(return_value=number_of_equities)
    mocker.patch.object(command, "_number_of_equities", mock_number_of_equities)
    number_of_etfs = 8
    mock_number_of_etfs = mocker.MagicMock(return_value=number_of_etfs)
    mocker.patch.object(command, "_number_of_etfs", mock_number_of_etfs)

    mock_open = mocker.MagicMock()
    mock_file = mocker.MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mocker.patch("pyp.cli.commands.output.summary.open", mock_open)

    command._write_data_files()

    summary = {
        "date": command.date.strftime("%d-%b-%Y"),
        "username": command.username,
        "portfolio": command.portfolio_name,
        "currency": command.currency_code,
        "invested": round(invested_value, 2),
        "value": round(market_value, 2),
        "percent": round(((market_value - invested_value) / invested_value) * 100, 2),
        "equities": number_of_equities,
        "etfs": number_of_etfs,
    }
    summary_json = json.dumps(summary)

    mock_open.assert_called_once_with(command.output_dir / "summary.js", "w")
    mock_file.write.assert_has_calls([
        call(f"summary_data = {summary_json}\n"),
        call(f"portfolio_data = {mock_df.to_json.return_value}\n"),
    ])
