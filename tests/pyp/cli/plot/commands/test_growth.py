from datetime import date, datetime
from pathlib import Path
from unittest.mock import call

import pandas as pd
import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable

from pyp.cli.plot.commands.base import PlotCommand
from pyp.cli.plot.commands.growth import PlotGrowth
from pyp.cli.plot.commands.protocols import PlotCommandProtocol


@pytest.fixture
def command(portfolio_id: int) -> PlotCommand:
    return PlotGrowth(portfolio_id, datetime(2024, 12, 3), output_dir=Path(__file__).parent)


@pytest.fixture
def share_value_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "ADP", "ADP", "IYK", "IYK", "IYK", "RDVY"],
            "amount": [1.749, 0.009, 0.008, 4.795, 1.000, 0.864, 19.065],
            "value": [432.107940, 2.092270, 1.970961, 863.690744, 187.400000, 162.215136, 863.545362],
            "market_price": [213.345108, 241.746033, 239.294220, 59.607300, 61.848171, 61.848171, 43.418995],
            "purchased_on": [
                date(2023, 10, 9),
                date(2024, 1, 3),
                date(2024, 4, 2),
                date(2023, 10, 6),
                date(2023, 11, 3),
                date(2023, 11, 3),
                date(2023, 10, 6),
            ],
        }
    ).astype(
        dtype={
            "moniker": "string",
            "amount": "float64",
            "value": "float64",
            "market_price": "float64",
            "purchased_on": "object",
        }
    )


@pytest.fixture
def invested_df(share_value_df: DataFrame) -> DataFrame:
    df = share_value_df.copy(deep=True)
    return df.rename(columns={"value": "invested"})


@pytest.fixture
def month_df(invested_df: DataFrame) -> DataFrame:
    df = invested_df.copy(deep=True)
    df["month"] = pd.to_datetime(df["purchased_on"]).dt.strftime("%Y-%m")

    return df.drop(columns=["purchased_on"])


@pytest.fixture
def monthly_df(month_df: DataFrame) -> DataFrame:
    df = month_df.copy(deep=True)

    return (
        df.groupby(["moniker", "month"])
        .agg({
            "amount": "sum",
            "invested": "sum",
            "market_price": "last",
        })
        .reset_index()
    )


@pytest.fixture
def market_value_df(monthly_df: DataFrame) -> DataFrame:
    df = monthly_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]

    return df.drop(columns=["amount", "market_price"])


@pytest.fixture
def summed_monthly_df(market_value_df: DataFrame) -> DataFrame:
    df = market_value_df.copy(deep=True)

    return (
        df.groupby(["month"])
        .agg({
            "invested": "sum",
            "value": "sum",
        })
        .reset_index()
    )


@pytest.fixture
def cumulative_sum_df(summed_monthly_df: DataFrame) -> DataFrame:
    df = summed_monthly_df.copy(deep=True)

    df["cum_sum_value"] = df["value"].cumsum()
    df["cum_sum_invested"] = df["invested"].cumsum()

    df = df.drop(columns=["invested", "value"]).rename(
        columns={"cum_sum_value": "value", "cum_sum_invested": "invested"}
    )

    return df


@pytest.fixture
def profit_df(cumulative_sum_df: DataFrame) -> DataFrame:
    df = cumulative_sum_df.copy(deep=True)
    df["profit"] = df["value"] - df["invested"]

    return df


@pytest.fixture
def profit_ratio_df(profit_df: DataFrame) -> DataFrame:
    df = profit_df.copy(deep=True)
    df["profit_ratio"] = df["profit"] / df["invested"]

    return df


def test_initialization(portfolio_id: int) -> None:
    output_dir = Path(__file__).parent
    date = datetime(2024, 12, 9)

    command = PlotGrowth(portfolio_id, date, output_dir=output_dir)

    assert isinstance(command, PlotCommand)
    assert isinstance(command, PlotCommandProtocol)


def test_db_query_property(command: PlotGrowth) -> None:
    db_query = command._db_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        shares.amount,
        shares.price,
        shares.purchased_on,
        (
            SELECT prices.amount
            FROM prices
            WHERE prices.stock_id = stocks.id
            AND strftime(:strftime_1, prices.date) = strftime(:strftime_2, shares.purchased_on)
            ORDER BY prices.date DESC
            LIMIT :param_1
        ) AS market_price
    FROM shares
    JOIN portfolio_stocks ON portfolio_stocks.id = shares.portfolio_stocks_id
    JOIN stocks ON stocks.id = portfolio_stocks.stock_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1
        AND shares.purchased_on <= :purchased_on_1"""

    expected_query = (
        query.replace("(\n            ", "(")
        .replace("\n            ", " ")
        .replace("\n            ", " ")
        .replace("\n        )", ")")
        .replace("\n        ", " ")
        .replace("\n    ", " ")
    )

    assert expected_query == str(db_query).replace("\n", "")


def test_df_dtypes_property(command: PlotGrowth) -> None:
    assert {
        "moniker": "string",
        "amount": "float64",
        "price": "float64",
        "market_price": "float64",
        "purchased_on": "object",
    } == command._df_dtypes


def test_rename_value_to_invested(command: PlotGrowth, share_value_df: DataFrame) -> None:
    command._df = share_value_df

    df = share_value_df.rename(columns={"value": "invested"})

    assert command == command._rename_value_to_invested()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_parse_purchased_on_to_month(command: PlotGrowth, invested_df: DataFrame) -> None:
    command._df = invested_df

    df = invested_df
    df["month"] = pd.to_datetime(df["purchased_on"]).dt.strftime("%Y-%m")
    df = df.drop(columns=["purchased_on"])

    assert command == command._parse_purchased_on_to_month()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_sum_up_by_month_and_moniker(command: PlotGrowth, month_df: DataFrame) -> None:
    command._df = month_df

    df = (
        month_df.groupby(["moniker", "month"])
        .agg({
            "amount": "sum",
            "invested": "sum",
            "market_price": "last",
        })
        .reset_index()
    )

    assert command == command._sum_up_by_month_and_moniker()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_market_value(command: PlotGrowth, monthly_df: DataFrame) -> None:
    command._df = monthly_df

    df = monthly_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]
    df = df.drop(columns=["amount", "market_price"])

    assert command == command._compute_market_value()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_sum_up_by_month(command: PlotGrowth, market_value_df: DataFrame) -> None:
    command._df = market_value_df

    df = (
        market_value_df.groupby(["month"])
        .agg({
            "invested": "sum",
            "value": "sum",
        })
        .reset_index()
    )

    assert command == command._sum_up_by_month()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_cumulative_sums(command: PlotGrowth, summed_monthly_df: DataFrame) -> None:
    command._df = summed_monthly_df

    df = summed_monthly_df
    df["cum_sum_value"] = df["value"].cumsum()
    df["cum_sum_invested"] = df["invested"].cumsum()
    df = df.drop(columns=["invested", "value"]).rename(
        columns={"cum_sum_value": "value", "cum_sum_invested": "invested"}
    )

    assert command == command._compute_cumulative_sums()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_profit(command: PlotGrowth, cumulative_sum_df: DataFrame) -> None:
    command._df = cumulative_sum_df

    df = cumulative_sum_df
    df["profit"] = df["value"] - df["invested"]

    assert command == command._compute_profit()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_compute_profit_ratio(command: PlotGrowth, profit_df: DataFrame) -> None:
    command._df = profit_df

    df = profit_df
    df["profit_ratio"] = df["profit"] / df["invested"]

    assert command == command._compute_profit_ratio()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_invested_vs_value_df(command: PlotGrowth, profit_ratio_df: DataFrame) -> None:
    command._df = profit_ratio_df

    df = profit_ratio_df[["month", "invested", "value"]]

    actual_df = command._invested_vs_value_df

    assert isinstance(actual_df, DataFrame)
    assert df.equals(actual_df)


def test_month_vs_profit_df(command: PlotGrowth, profit_ratio_df: DataFrame) -> None:
    command._df = profit_ratio_df

    df = profit_ratio_df[["month", "profit"]]

    actual_df = command._month_vs_profit_df

    assert isinstance(actual_df, DataFrame)
    assert df.equals(actual_df)


def test_month_vs_profit_ratio_df(command: PlotGrowth, profit_ratio_df: DataFrame) -> None:
    command._df = profit_ratio_df

    df = profit_ratio_df[["month", "profit_ratio"]]

    actual_df = command._month_vs_profit_ratio_df

    assert isinstance(actual_df, DataFrame)
    assert df.equals(actual_df)


def test_prepare_df(command: PlotGrowth, mocker: MockFixture) -> None:
    mock_csv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_share_value", mock_csv)
    mock_rvti = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_rename_value_to_invested", mock_rvti)
    mock_ppotm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_parse_purchased_on_to_month", mock_ppotm)
    mock_submam = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_sum_up_by_month_and_moniker", mock_submam)
    mock_cmv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_market_value", mock_cmv)
    mock_subm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_sum_up_by_month", mock_subm)
    mock_ccs = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_cumulative_sums", mock_ccs)
    mock_cp = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit", mock_cp)
    mock_cpr = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit_ratio", mock_cpr)

    command._prepare_df()

    mock_csv.assert_called_once()
    mock_rvti.assert_called_once()
    mock_ppotm.assert_called_once()
    mock_submam.assert_called_once()
    mock_cmv.assert_called_once()
    mock_subm.assert_called_once()
    mock_ccs.assert_called_once()
    mock_cp.assert_called_once()
    mock_cpr.assert_called_once()


def test_writes_json_files(command: PlotGrowth, mocker: MockFixture) -> None:
    mock_growth_df = mocker.MagicMock()
    mock_growth_df.to_json = mocker.MagicMock()
    command._df = mock_growth_df

    command._write_json_files()
    assert command.output_dir is not None

    mock_growth_df.to_json.assert_called_once_with(command.output_dir / "growth.json", orient="records")


def test_show(command: PlotGrowth, mocker: MockFixture) -> None:
    mock_area = mocker.MagicMock()
    mock_plot = mocker.MagicMock()
    mock_plot.area = mock_area
    mock_plot.bar = mocker.MagicMock()

    mock_ivvdf = mocker.MagicMock()
    mock_ivvdf.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth._invested_vs_value_df", mock_ivvdf)
    mock_mvpdf = mocker.MagicMock()
    mock_mvpdf.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth._month_vs_profit_df", mock_mvpdf)
    mock_mvprdf = mocker.MagicMock()
    mock_mvprdf.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth._month_vs_profit_ratio_df", mock_mvprdf)

    mock_show = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.growth.plt.show", mock_show)

    command._show()

    mock_area.assert_has_calls([
        call(x="month", stacked=False, title="Invested vs. Market"),
        call(x="month", stacked=False, title="Profit Growth"),
    ])
    mock_plot.bar.assert_called_once_with(x="month", title="Profit Ratio")
    mock_show.assert_called_once()
