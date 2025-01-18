from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from pyp.cli.commands.output.base import OutputCommand
from pyp.cli.commands.output.growth_breakdown import (
    OutputGrowthBreakdownCommand,
    OutputGrowthBreakdownMonthOverMonthCommand,
)
from pyp.cli.protocols import OutputCommandProtocol


@pytest.fixture
def with_exchange_rates_df(
    added_monthly_market_prices_df: DataFrame, formatted_exchange_rates_df: DataFrame
) -> DataFrame:
    df = added_monthly_market_prices_df.copy(deep=True)
    df = df.merge(
        formatted_exchange_rates_df,
        how="left",
        left_on=["month", "currency_id"],
        right_on=["month", "from_currency_id"],
    )

    df["rate"] = df["rate"].fillna(1)

    df = df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

    return df


@pytest.fixture
def converted_to_currency(with_exchange_rates_df: DataFrame) -> DataFrame:
    df = with_exchange_rates_df.copy(deep=True)
    df["invested"] = df["invested"] * df["rate"]
    df["market_price"] = df["market_price"] * df["rate"]

    df = df.drop(columns=["rate"])

    return df


@pytest.fixture
def calculated_monthly_difference_df(converted_to_currency: DataFrame) -> DataFrame:
    df = converted_to_currency.copy(deep=True)
    df["price_difference"] = df.groupby(["moniker"], sort=False)["market_price"].diff().fillna(0)
    df["last_market_price"] = df.groupby(["moniker"], sort=False)["market_price"].shift().fillna(0)
    df["month_over_month_ratio"] = (df["price_difference"] / df["last_market_price"]).fillna(0)
    df = df.drop(columns=["price_difference", "last_market_price"])

    return df


@pytest.fixture
def command(portfolio_id: int, mock_engine: MagicMock) -> OutputCommand:
    return OutputGrowthBreakdownMonthOverMonthCommand(mock_engine, portfolio_id, datetime(2024, 12, 3), "USD")


def test_initialization(portfolio_id: int, mock_engine: MagicMock) -> None:
    date = datetime(2024, 12, 9)
    currency_code = "USD"

    command = OutputGrowthBreakdownMonthOverMonthCommand(mock_engine, portfolio_id, date, currency_code)

    assert isinstance(command, OutputGrowthBreakdownCommand)
    assert isinstance(command, OutputCommandProtocol)

    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert currency_code == command.currency_code


def test_calculate_monthly_difference(
    command: OutputGrowthBreakdownMonthOverMonthCommand, converted_to_currency: DataFrame
) -> None:
    command._df = converted_to_currency

    df = converted_to_currency.copy(deep=True)
    df["price_difference"] = df.groupby(["moniker"], sort=False)["market_price"].diff().fillna(0)
    df["last_market_price"] = df.groupby(["moniker"], sort=False)["market_price"].shift().fillna(0)
    df["month_over_month_ratio"] = (df["price_difference"] / df["last_market_price"]).fillna(0)
    df = df.drop(columns=["price_difference", "last_market_price"])

    assert command == command._calculate_monthly_difference()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_convert_to_currency(
    command: OutputGrowthBreakdownMonthOverMonthCommand, with_exchange_rates_df: DataFrame
) -> None:
    command._df = with_exchange_rates_df

    df = with_exchange_rates_df.copy(deep=True)
    df["invested"] = df["invested"] * df["rate"]
    df["market_price"] = df["market_price"] * df["rate"]

    df = df.drop(columns=["rate"])

    command._convert_to_currency()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_prepare_df(command: OutputGrowthBreakdownMonthOverMonthCommand, mocker: MockerFixture) -> None:
    mock_rdb = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_read_db", mock_rdb)
    mock_csv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_share_value", mock_csv)
    mock_rvti = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_rename_value_to_invested", mock_rvti)
    mock_submam = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_sum_up_by_month_and_moniker", mock_submam)
    mock_ammp = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_add_monthly_market_prices", mock_ammp)
    mock_rci = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_resolve_currency_ids", mock_rci)
    mock_aer = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_add_exchange_rates", mock_aer)
    mock_ctc = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_convert_to_currency", mock_ctc)
    mock_cmd = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_calculate_monthly_difference", mock_cmd)

    command._prepare_df()

    mock_rdb.assert_called_once()
    mock_csv.assert_called_once()
    mock_rvti.assert_called_once()
    mock_submam.assert_called_once()
    mock_ammp.assert_called_once()
    mock_rci.assert_called_once()
    mock_aer.assert_called_once()
    mock_ctc.assert_called_once()
    mock_cmd.assert_called_once()


def test_equity_month_over_month_ratio_df(
    command: OutputGrowthBreakdownMonthOverMonthCommand, calculated_monthly_difference_df: DataFrame
) -> None:
    command._df = calculated_monthly_difference_df

    df = calculated_monthly_difference_df.copy(deep=True)
    df = df[df["stock_type"] == "EQUITY"].copy(deep=True)
    df = df[["month", "moniker", "month_over_month_ratio"]]

    data = {
        "month": df["month"].unique(),
    }

    for moniker in df["moniker"].unique():
        data[moniker] = df[df["moniker"] == moniker]["month_over_month_ratio"].to_list()

    expected_df = DataFrame(data=data)

    actual_df = command._equity_month_over_month_ratio_df

    assert isinstance(actual_df, DataFrame)
    assert expected_df.equals(actual_df)


def test_etf_month_over_month_ratio_df(
    command: OutputGrowthBreakdownMonthOverMonthCommand, calculated_monthly_difference_df: DataFrame
) -> None:
    command._df = calculated_monthly_difference_df

    df = calculated_monthly_difference_df.copy(deep=True)
    df = df[df["stock_type"] == "ETF"].copy(deep=True)
    df = df[["month", "moniker", "month_over_month_ratio"]]

    data = {
        "month": df["month"].unique(),
    }

    for moniker in df["moniker"].unique():
        data[moniker] = df[df["moniker"] == moniker]["month_over_month_ratio"].to_list()

    expected_df = DataFrame(data=data)

    actual_df = command._etf_month_over_month_ratio_df

    assert isinstance(actual_df, DataFrame)
    assert expected_df.equals(actual_df)


def test_writes_data_files(command: OutputGrowthBreakdownMonthOverMonthCommand, mocker: MockerFixture) -> None:
    mock_equity_month_over_month_ratio_df = mocker.MagicMock()
    mock_equity_month_over_month_ratio_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.growth_breakdown.OutputGrowthBreakdownMonthOverMonthCommand._equity_month_over_month_ratio_df",
        mock_equity_month_over_month_ratio_df,
    )

    mock_etf_month_over_month_ratio_df = mocker.MagicMock()
    mock_etf_month_over_month_ratio_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.growth_breakdown.OutputGrowthBreakdownMonthOverMonthCommand._etf_month_over_month_ratio_df",
        mock_etf_month_over_month_ratio_df,
    )

    mock_open = mocker.MagicMock()
    mock_file = mocker.MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mocker.patch("pyp.cli.commands.output.growth_breakdown.open", mock_open)

    command._write_data_files()
    assert command.output_dir is not None

    mock_equity_month_over_month_ratio_df.to_json.assert_called_once_with(orient="records")

    growth_breakdown_by_stock_type = (
        f'{{"EQUITY": {mock_equity_month_over_month_ratio_df.to_json.return_value}, '
        f'"ETF": {mock_etf_month_over_month_ratio_df.to_json.return_value}}}'
    )

    mock_open.assert_called_once_with(command.output_dir / "breakdown.js", "a")
    mock_file.write.assert_called_once_with(
        f"growth_breakdown_mom_by_stock_type_data = {growth_breakdown_by_stock_type}\n"
    )
