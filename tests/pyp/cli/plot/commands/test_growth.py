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


def test_initialization(portfolio_id: int) -> None:
    output_dir = Path(__file__).parent
    date = datetime(2024, 12, 9)

    command = PlotGrowth(portfolio_id, date, output_dir=output_dir)

    assert isinstance(command, PlotCommand)
    assert isinstance(command, PlotCommandProtocol)


def test_db_query_property(command: PlotGrowth) -> None:
    db_query = command.db_query

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


def test_db_data_df_dtypes_property(command: PlotGrowth) -> None:
    assert {
        "moniker": "string",
        "amount": "float64",
        "price": "float64",
        "market_price": "float64",
        "purchased_on": "object",
    } == command.db_data_df_dtypes


def test_invested_df(command: PlotGrowth, share_value_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=share_value_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.share_value_df", mock_property)

    df = share_value_df.copy(deep=True)
    df = df.rename(columns={"value": "invested"})

    invested_df = command.invested_df

    assert isinstance(invested_df, DataFrame)
    assert df.equals(invested_df)


def test_month_df(command: PlotGrowth, invested_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=invested_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.invested_df", mock_property)

    df = invested_df.copy(deep=True)
    df["month"] = pd.to_datetime(df["purchased_on"]).dt.strftime("%Y-%m")
    df = df.drop(columns=["purchased_on"])

    month_df = command.month_df

    assert isinstance(month_df, DataFrame)
    assert df.equals(month_df)


def test_monthly_df(command: PlotGrowth, month_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=month_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.month_df", mock_property)

    df = month_df.copy(deep=True)
    df = (
        df.groupby(["moniker", "month"])
        .agg({
            "amount": "sum",
            "invested": "sum",
            "market_price": "last",
        })
        .reset_index()
    )

    monthly_df = command.monthly_df

    assert isinstance(monthly_df, DataFrame)
    assert df.equals(monthly_df)


def test_market_value_df(command: PlotGrowth, monthly_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=monthly_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.monthly_df", mock_property)

    df = monthly_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]
    df = df.drop(columns=["amount", "market_price"])

    market_value_df = command.market_value_df

    assert isinstance(market_value_df, DataFrame)
    assert df.equals(market_value_df)


def test_summed_monthly_df(command: PlotGrowth, market_value_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=market_value_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.market_value_df", mock_property)

    df = market_value_df.copy(deep=True)
    df = (
        df.groupby(["month"])
        .agg({
            "invested": "sum",
            "value": "sum",
        })
        .reset_index()
    )

    summed_monthly_df = command.summed_monthly_df

    assert isinstance(summed_monthly_df, DataFrame)
    assert df.equals(summed_monthly_df)


def test_cumulative_sum_df(command: PlotGrowth, summed_monthly_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=summed_monthly_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.summed_monthly_df", mock_property)

    df = summed_monthly_df.copy(deep=True)
    df["cum_sum_value"] = df["value"].cumsum()
    df["cum_sum_invested"] = df["invested"].cumsum()
    df = df.drop(columns=["invested", "value"]).rename(
        columns={"cum_sum_value": "value", "cum_sum_invested": "invested"}
    )

    cumulative_sum_df = command.cumulative_sum_df

    assert isinstance(cumulative_sum_df, DataFrame)
    assert df.equals(cumulative_sum_df)


def test_profit_df(command: PlotGrowth, cumulative_sum_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=cumulative_sum_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.cumulative_sum_df", mock_property)

    df = cumulative_sum_df.copy(deep=True)
    df["profit"] = df["value"] - df["invested"]

    profit_df = command.profit_df

    assert isinstance(profit_df, DataFrame)
    assert df.equals(profit_df)


def test_growth_df(command: PlotGrowth, profit_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=profit_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.profit_df", mock_property)

    df = profit_df.copy(deep=True)
    df["profit_ratio"] = df["profit"] / df["invested"]

    growth_df = command.growth_df

    assert isinstance(growth_df, DataFrame)
    assert df.equals(growth_df)


def test_writes_json_files(command: PlotGrowth, mocker: MockFixture) -> None:
    mock_growth_df = mocker.MagicMock()
    mock_growth_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.growth_df", mock_growth_df)

    command.write_json_files()
    assert command.output_dir is not None

    mock_growth_df.to_json.assert_called_once_with(command.output_dir / "growth.json", orient="records")


def test_show(command: PlotGrowth, mocker: MockFixture) -> None:
    mock_area = mocker.MagicMock()
    mock_plot = mocker.MagicMock()
    mock_plot.area = mock_area
    mock_plot.bar = mocker.MagicMock()
    mock_df = mocker.MagicMock()
    mock_df.copy = mocker.MagicMock(return_value=mock_df)
    mock_df.__getitem__ = mocker.MagicMock(return_value=mock_df)
    mock_df.plot = mock_plot
    mock_property = mocker.PropertyMock(return_value=mock_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.growth_df", mock_property)

    mock_show = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.growth.plt.show", mock_show)

    command.show()

    mock_area.assert_has_calls([
        call(x="month", stacked=False, title="Invested vs. Market"),
        call(x="month", stacked=False, title="Profit Growth"),
    ])
    mock_plot.bar.assert_called_once_with(x="month", title="Profit Ratio")
    mock_show.assert_called_once()
