from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from pyp.cli.commands.output.base import OutputCommand
from pyp.cli.commands.output.growth import OutputGrowthCommand
from pyp.cli.commands.output.growth_breakdown import OutputGrowthBreakdownCommand
from pyp.cli.protocols import OutputCommandProtocol


@pytest.fixture
def summed_monthly_df(market_value_df: DataFrame) -> DataFrame:
    df = market_value_df.copy(deep=True)

    return (
        df.groupby(["moniker", "month"])
        .agg({
            "stock_type": "first",
            "invested": "sum",
            "value": "sum",
        })
        .reset_index()
    )


@pytest.fixture
def cumulative_sum_df(summed_monthly_df: DataFrame) -> DataFrame:
    df = summed_monthly_df.copy(deep=True)

    df["cum_sum_invested"] = df.groupby(["moniker"])["invested"].cumsum()

    df = df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

    return df


@pytest.fixture
def command(portfolio_id: int, mock_engine: MagicMock) -> OutputCommand:
    return OutputGrowthBreakdownCommand(mock_engine, portfolio_id, datetime(2024, 12, 3), "USD")


def test_initialization(portfolio_id: int, mock_engine: MagicMock) -> None:
    date = datetime(2024, 12, 9)
    currency_code = "USD"

    command = OutputGrowthBreakdownCommand(mock_engine, portfolio_id, date, currency_code)

    assert isinstance(command, OutputGrowthCommand)
    assert isinstance(command, OutputCommandProtocol)

    assert mock_engine == command.engine
    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert currency_code == command.currency_code


def test_compute_cumulative_sums(command: OutputGrowthBreakdownCommand, summed_monthly_df: DataFrame) -> None:
    command._df = summed_monthly_df

    df = summed_monthly_df.copy(deep=True)
    df["cum_sum_invested"] = df.groupby(["moniker"])["invested"].cumsum()
    df = df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

    assert command == command._compute_cumulative_sums()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_prepare_df(command: OutputGrowthBreakdownCommand, mocker: MockerFixture) -> None:
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
    mock_cmv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_market_value", mock_cmv)
    mock_rci = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_resolve_currency_ids", mock_rci)
    mock_aer = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_add_exchange_rates", mock_aer)
    mock_ctc = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_convert_to_currency", mock_ctc)
    mock_ccs = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_cumulative_sums", mock_ccs)
    mock_cp = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit", mock_cp)
    mock_cpr = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit_ratio", mock_cpr)
    mock_cprd = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit_ratio_difference", mock_cprd)

    command._prepare_df()

    mock_rdb.assert_called_once()
    mock_csv.assert_called_once()
    mock_rvti.assert_called_once()
    mock_submam.assert_called_once()
    mock_ammp.assert_called_once()
    mock_cmv.assert_called_once()
    mock_rci.assert_called_once()
    mock_aer.assert_called_once()
    mock_ctc.assert_called_once()
    mock_ccs.assert_called_once()
    mock_cp.assert_called_once()
    mock_cpr.assert_called_once()
    mock_cprd.assert_called_once()


def test_equity_month_vs_profit_ratio_df(command: OutputGrowthBreakdownCommand, profit_ratio_df: DataFrame) -> None:
    command._df = profit_ratio_df

    df = profit_ratio_df.copy(deep=True)
    df = df[df["stock_type"] == "EQUITY"].copy(deep=True)
    df = df[["month", "moniker", "profit_ratio"]]

    data = {
        "month": df["month"].unique(),
    }

    for moniker in df["moniker"].unique():
        data[moniker] = df[df["moniker"] == moniker]["profit_ratio"].to_list()

    expected_df = DataFrame(data=data)

    actual_df = command._equity_month_vs_profit_ratio_df

    assert isinstance(actual_df, DataFrame)
    assert expected_df.equals(actual_df)


def test_etf_month_vs_profit_ratio_df(command: OutputGrowthBreakdownCommand, profit_ratio_df: DataFrame) -> None:
    command._df = profit_ratio_df

    df = profit_ratio_df.copy(deep=True)
    df = df[df["stock_type"] == "ETF"].copy(deep=True)
    df = df[["month", "moniker", "profit_ratio"]]

    data = {
        "month": df["month"].unique(),
    }

    for moniker in df["moniker"].unique():
        data[moniker] = df[df["moniker"] == moniker]["profit_ratio"].to_list()

    expected_df = DataFrame(data=data)

    actual_df = command._etf_month_vs_profit_ratio_df

    assert isinstance(actual_df, DataFrame)
    assert expected_df.equals(actual_df)


def test_writes_data_files(command: OutputGrowthBreakdownCommand, mocker: MockerFixture) -> None:
    mock_equity_month_vs_profit_ratio_df = mocker.MagicMock()
    mock_equity_month_vs_profit_ratio_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.growth_breakdown.OutputGrowthBreakdownCommand._equity_month_vs_profit_ratio_df",
        mock_equity_month_vs_profit_ratio_df,
    )

    mock_etf_month_vs_profit_ratio_df = mocker.MagicMock()
    mock_etf_month_vs_profit_ratio_df.to_json = mocker.MagicMock()
    mocker.patch(
        "pyp.cli.commands.output.growth_breakdown.OutputGrowthBreakdownCommand._etf_month_vs_profit_ratio_df",
        mock_etf_month_vs_profit_ratio_df,
    )

    mock_open = mocker.MagicMock()
    mock_file = mocker.MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mocker.patch("pyp.cli.commands.output.growth_breakdown.open", mock_open)

    command._write_data_files()
    assert command.output_dir is not None

    mock_equity_month_vs_profit_ratio_df.to_json.assert_called_once_with(orient="records")

    growth_breakdown_by_stock_type = (
        f'{{"EQUITY": {mock_equity_month_vs_profit_ratio_df.to_json.return_value}, '
        f'"ETF": {mock_etf_month_vs_profit_ratio_df.to_json.return_value}}}'
    )

    mock_open.assert_called_once_with(command.output_dir / "breakdown.js", "a")
    mock_file.write.assert_called_once_with(f"growth_breakdown_by_stock_type_data = {growth_breakdown_by_stock_type}\n")
