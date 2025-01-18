from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture
from sqlalchemy import Selectable

from pyp.cli.commands.output.base import OutputCommand
from pyp.cli.commands.output.growth import OutputGrowthCommand
from pyp.cli.protocols import OutputCommandProtocol
from pyp.database.models import Currency


@pytest.fixture
def command(portfolio_id: int, mock_engine: MagicMock) -> OutputCommand:
    return OutputGrowthCommand(mock_engine, portfolio_id, datetime(2024, 12, 3), "USD")


def test_initialization(portfolio_id: int, mock_engine: MagicMock) -> None:
    date = datetime(2024, 12, 9)
    currency_code = "USD"

    command = OutputGrowthCommand(mock_engine, portfolio_id, date, currency_code)

    assert isinstance(command, OutputCommand)
    assert isinstance(command, OutputCommandProtocol)

    assert mock_engine == command.engine
    assert portfolio_id == command.portfolio_id
    assert date == command.date
    assert currency_code == command.currency_code


def test_db_query_property(command: OutputGrowthCommand) -> None:
    db_query = command._db_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        stocks.stock_type,
        shares.amount,
        shares.price,
        strftime(:strftime_1, shares.purchased_on) AS month
    FROM shares
    JOIN portfolio_stocks ON portfolio_stocks.id = shares.portfolio_stocks_id
    JOIN stocks ON stocks.id = portfolio_stocks.stock_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1
        AND shares.purchased_on <= :purchased_on_1
    ORDER BY shares.purchased_on"""

    expected_query = (
        query.replace("(\n            ", "(")
        .replace("\n            ", " ")
        .replace("\n            ", " ")
        .replace("\n        )", ")")
        .replace("\n        ", " ")
        .replace("\n    ", " ")
    )

    assert expected_query == str(db_query).replace("\n", "")


def test_df_dtypes_property(command: OutputGrowthCommand) -> None:
    assert {
        "moniker": "string",
        "stock_type": "string",
        "amount": "float64",
        "price": "float64",
        "month": "string",
    } == command._df_dtypes


def test_rename_value_to_invested(command: OutputGrowthCommand, share_value_df: DataFrame) -> None:
    command._df = share_value_df

    df = share_value_df.copy(deep=True).rename(columns={"value": "invested"})

    assert command == command._rename_value_to_invested()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_sum_up_by_month_and_moniker(command: OutputGrowthCommand, invested_df: DataFrame) -> None:
    command._df = invested_df

    df = (
        invested_df.copy(deep=True)
        .groupby(["moniker", "month"], sort=False)
        .agg({
            "stock_type": "first",
            "amount": "sum",
            "invested": "sum",
        })
        .reset_index()
    )

    assert command == command._sum_up_by_month_and_moniker()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_monthly_prices_query_property(command: OutputGrowthCommand) -> None:
    db_query = command._monthly_prices_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        stocks.stock_type,
        stocks.currency_id,
        strftime(:strftime_1, max(prices.date)) AS month,
        prices.amount AS market_price
    FROM prices
    JOIN stocks ON stocks.id = prices.stock_id
    JOIN portfolio_stocks ON stocks.id = portfolio_stocks.stock_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1 AND prices.date <= :date_1
    GROUP BY stocks.moniker, strftime(:strftime_2, prices.date)
    ORDER BY prices.date"""

    expected_query = (
        query.replace("(\n            ", "(")
        .replace("\n            ", " ")
        .replace("\n            ", " ")
        .replace("\n        )", ")")
        .replace("\n        ", " ")
        .replace("\n    ", " ")
    )

    assert expected_query == str(db_query).replace("\n", "")


def test_monthly_prices_df(
    command: OutputGrowthCommand,
    monthly_prices_df: DataFrame,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.commands.output.growth.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=monthly_prices_df)
    mocker.patch("pyp.cli.commands.output.growth.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.commands.output.growth.OutputGrowthCommand._monthly_prices_query", mock_property)

    assert monthly_prices_df.equals(command._monthly_prices_df)

    mock_session_class.assert_called_once_with(command.engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_add_monthly_market_prices(
    command: OutputGrowthCommand, monthly_prices_df: DataFrame, monthly_df: DataFrame, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.commands.output.growth.OutputGrowthCommand._monthly_prices_df", monthly_prices_df)

    command._df = monthly_df

    expected_df = monthly_prices_df.copy(deep=True).merge(
        monthly_df,
        how="left",
        left_on=["moniker", "month", "stock_type"],
        right_on=["moniker", "month", "stock_type"],
    )

    expected_df[["amount", "invested"]] = expected_df[["amount", "invested"]].fillna(0, axis="columns")
    expected_df["amount"] = expected_df.groupby("moniker")["amount"].cumsum()

    expected_df["month"] = expected_df["month"].apply(lambda x: datetime.strptime(x, "%Y-%m").strftime("%b-%y"))

    assert command == command._add_monthly_market_prices()

    assert isinstance(command._df, DataFrame)
    assert expected_df.equals(command._df)


def test_compute_market_value(command: OutputGrowthCommand, added_monthly_market_prices_df: DataFrame) -> None:
    command._df = added_monthly_market_prices_df

    df = added_monthly_market_prices_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]
    df = df.drop(columns=["amount", "market_price"]).reset_index()

    assert command == command._compute_market_value()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_convert_to_currency(command: OutputGrowthCommand, with_exchange_rates_df: DataFrame) -> None:
    command._df = with_exchange_rates_df

    df = with_exchange_rates_df.copy(deep=True)
    df["invested"] = df["invested"] * df["rate"]
    df["value"] = df["value"] * df["rate"]

    df = df.drop(columns=["rate"])

    command._convert_to_currency()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_sum_up_by_month(command: OutputGrowthCommand, converted_to_currency: DataFrame) -> None:
    command._df = converted_to_currency

    df = (
        converted_to_currency.copy(deep=True)
        .groupby(["month"], sort=False)
        .agg({
            "stock_type": "first",
            "invested": "sum",
            "value": "sum",
        })
        .reset_index()
    )

    assert command == command._sum_up_by_month()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_cumulative_sums(command: OutputGrowthCommand, summed_monthly_df: DataFrame) -> None:
    command._df = summed_monthly_df

    df = summed_monthly_df.copy(deep=True)
    df["cum_sum_invested"] = df["invested"].cumsum()
    df = df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

    assert command == command._compute_cumulative_sums()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_profit(command: OutputGrowthCommand, cumulative_sum_df: DataFrame) -> None:
    command._df = cumulative_sum_df

    df = cumulative_sum_df.copy(deep=True)
    df["profit"] = df["value"] - df["invested"]

    assert command == command._compute_profit()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_profit_ratio(command: OutputGrowthCommand, profit_df: DataFrame) -> None:
    command._df = profit_df

    df = profit_df.copy(deep=True)
    df["profit_ratio"] = df["profit"] / df["invested"]
    df["profit_ratio"] = df["profit_ratio"].fillna(0)

    assert command == command._compute_profit_ratio()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_profit_ratio_difference(command: OutputGrowthCommand, profit_ratio_df: DataFrame) -> None:
    command._df = profit_ratio_df

    df = profit_ratio_df.copy(deep=True)
    df["profit_ratio_difference"] = df["profit_ratio"] - df["profit_ratio"].shift()

    assert command == command._compute_profit_ratio_difference()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_prepare_df(command: OutputGrowthCommand, mocker: MockerFixture) -> None:
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
    mock_subm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_sum_up_by_month", mock_subm)
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
    mock_subm.assert_called_once()
    mock_ccs.assert_called_once()
    mock_cp.assert_called_once()
    mock_cpr.assert_called_once()
    mock_cprd.assert_called_once()


def test_prepare_df_full_check(
    command: OutputGrowthCommand,
    share_value_df: DataFrame,
    monthly_prices_df: DataFrame,
    formatted_exchange_rates_df: DataFrame,
    profit_ratio_difference_df: DataFrame,
    mocker: MockerFixture,
) -> None:
    mock_rdb = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_read_db", mock_rdb)
    mock_csv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_share_value", mock_csv)
    mocker.patch(
        "pyp.cli.commands.output.growth.OutputGrowthCommand._monthly_prices_df",
        mocker.PropertyMock(return_value=monthly_prices_df),
    )
    mock_rci = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_resolve_currency_ids", mock_rci)
    mocker.patch(
        "pyp.cli.commands.output.growth.OutputGrowthCommand._exchange_rates_df",
        mocker.PropertyMock(return_value=formatted_exchange_rates_df),
    )

    currencies = [Currency(id=1, code="USD"), Currency(id=2, code="CAD")]
    command._currency_ids_by_code = {c.code: c.id for c in currencies}
    command._df = share_value_df

    command._prepare_df()

    assert profit_ratio_difference_df.equals(command._df)


def test_writes_data_files(command: OutputGrowthCommand, mocker: MockerFixture) -> None:
    mock_growth_df = mocker.MagicMock()
    mock_growth_df.drop = mocker.MagicMock(return_value=mock_growth_df)
    mock_growth_df.to_json = mocker.MagicMock()
    command._df = mock_growth_df

    mock_open = mocker.MagicMock()
    mock_file = mocker.MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mocker.patch("pyp.cli.commands.output.growth.open", mock_open)

    command._write_data_files()
    assert command.output_dir is not None

    mock_growth_df.to_json.assert_called_once_with(orient="records")

    mock_open.assert_called_once_with(command.output_dir / "growth.js", "w")
    mock_file.write.assert_called_once_with(f"growth_data = {mock_growth_df.to_json.return_value}")
