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

    """
        moniker  amount purchased_on  market_price    invested
    0      ADP   1.749   2023-10-09    213.345108  432.107940
    1      ADP   0.009   2024-01-03    241.746033    2.092270
    2      ADP   0.008   2024-04-02    239.294220    1.970961
    3      ADP   0.009   2024-07-01    261.299347    2.119738
    4      ADP   0.007   2024-10-01    289.239990    1.963107
    5     BTCO   5.000   2024-02-28           NaN  305.925000
    6     BTCO   5.000   2024-03-05           NaN  338.900000
    7     BTCO   1.000   2024-03-06           NaN   66.050000
    8     PLTR   6.550   2024-11-06           NaN  352.053985
    9      IYK   4.795   2023-10-06     59.607300  863.690744
    10     IYK   1.000   2023-11-03     61.848171  187.400000
    11     IYK   0.864   2023-11-03     61.848171  162.215136
    12     IYK   0.051   2023-11-06     61.848171    9.548036
    13     IYK   0.046   2023-12-30     62.784698    8.903949
    14     IYK   0.072   2024-03-27     66.729584    4.852015
    15     IYK   3.150   2024-05-03     65.979698  208.997145
    16     IYK   0.120   2024-06-17     65.268906    7.931580
    17     IYK   5.142   2024-08-07     70.272026  357.094417
    18     IYK   5.000   2024-09-05     70.570000  358.095500
    19     IYK   0.207   2024-09-30     70.570000   14.572427
    20    RDVY  19.065   2023-10-06     43.418995  863.545362
    21    RDVY   0.106   2024-01-03     50.732254    5.436729
    22    RDVY   8.229   2024-02-28     52.204330  433.901181
    23    RDVY   0.091   2024-03-28     55.718300    5.116903
    24    RDVY   2.765   2024-05-03     54.805698  149.917194
    25    RDVY   6.567   2024-06-05     54.570816  358.742076
    26    RDVY   0.146   2024-07-01     58.268673    8.023912
    27    RDVY   0.089   2024-09-05     59.209999    5.056891
    28    RDVY   0.101   2024-09-30     59.209999    5.942517
    29    RDVY   6.081   2024-10-04     59.040001  361.301399

    """


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
def profit_df(summed_monthly_df: DataFrame) -> DataFrame:
    df = summed_monthly_df.copy(deep=True)
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


def test_profit_df(command: PlotGrowth, summed_monthly_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=summed_monthly_df)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth.summed_monthly_df", mock_property)

    df = summed_monthly_df.copy(deep=True)
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
