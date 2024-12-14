from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable

from pyp.cli.plot.commands.base import PlotCommand
from pyp.cli.plot.commands.growth import PlotGrowth
from pyp.cli.protocols import PlotCommandProtocol
from pyp.database.engine import engine


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
            "month": [
                "2023-10",
                "2024-01",
                "2024-04",
                "2023-10",
                "2023-11",
                "2023-11",
                "2023-10",
            ],
        }
    ).astype(
        dtype={
            "moniker": "string",
            "amount": "float64",
            "value": "float64",
            "month": "string",
        }
    )


@pytest.fixture
def invested_df(share_value_df: DataFrame) -> DataFrame:
    df = share_value_df.copy(deep=True)
    return df.rename(columns={"value": "invested"})


@pytest.fixture
def monthly_df(invested_df: DataFrame) -> DataFrame:
    df = invested_df.copy(deep=True)

    return (
        df.groupby(["moniker", "month"])
        .agg({
            "amount": "sum",
            "invested": "sum",
        })
        .reset_index()
    )


@pytest.fixture
def monthly_prices_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": [
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
            ],
            "month": [
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
            ],
            "market_price": [
                213.34510803222656,
                224.78372192382812,
                229.14627075195312,
                241.74603271484375,
                247.0082244873047,
                247.0599822998047,
                239.29421997070312,
                242.2917022705078,
                237.4897003173828,
                261.2993469238281,
                274.52252197265625,
                276.7300109863281,
                289.239990234375,
                306.92999267578125,
                296.9599914550781,
                59.6072998046875,
                61.84817123413086,
                62.784698486328125,
                63.33489990234375,
                64.09796905517578,
                66.72958374023438,
                65.98956298828125,
                65.97969818115234,
                65.26890563964844,
                67.4031753540039,
                70.27202606201172,
                70.56999969482422,
                68.31999969482422,
                70.91999816894531,
                68.60919952392578,
                43.41899490356445,
                47.36438751220703,
                51.04840087890625,
                50.73225402832031,
                52.20433044433594,
                55.718299865722656,
                52.89122009277344,
                54.80569839477539,
                54.57081604003906,
                58.268672943115234,
                58.55772399902344,
                59.209999084472656,
                59.040000915527344,
                64.29000091552734,
                62.4119987487793,
            ],
        }
    ).astype(dtype={"moniker": "string", "month": "string", "market_price": "float64"})


@pytest.fixture
def added_monthly_market_prices_df(monthly_prices_df: DataFrame, monthly_df: DataFrame) -> DataFrame:
    df = monthly_prices_df.merge(monthly_df, how="left", left_on=["moniker", "month"], right_on=["moniker", "month"])

    df[["amount", "invested"]] = df[["amount", "invested"]].fillna(0, axis="columns")
    df["amount"] = df.groupby("moniker")["amount"].cumsum()

    return df


@pytest.fixture
def market_value_df(added_monthly_market_prices_df: DataFrame) -> DataFrame:
    df = added_monthly_market_prices_df.copy(deep=True)
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
        strftime(:strftime_1, shares.purchased_on) AS month
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
        "month": "string",
    } == command._df_dtypes


def test_rename_value_to_invested(command: PlotGrowth, share_value_df: DataFrame) -> None:
    command._df = share_value_df

    df = share_value_df.rename(columns={"value": "invested"})

    assert command == command._rename_value_to_invested()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_sum_up_by_month_and_moniker(command: PlotGrowth, invested_df: DataFrame) -> None:
    command._df = invested_df

    df = (
        invested_df.groupby(["moniker", "month"])
        .agg({
            "amount": "sum",
            "invested": "sum",
        })
        .reset_index()
    )

    assert command == command._sum_up_by_month_and_moniker()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_monthly_prices_query_property(command: PlotGrowth) -> None:
    db_query = command._monthly_prices_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        strftime(:strftime_1, max(prices.date)) AS month,
        prices.amount AS market_price
    FROM prices
    JOIN stocks ON stocks.id = prices.stock_id
    JOIN portfolio_stocks ON stocks.id = portfolio_stocks.stock_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1 AND prices.date <= :date_1
    GROUP BY stocks.moniker, strftime(:strftime_2, prices.date)"""

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
    command: PlotGrowth,
    monthly_prices_df: DataFrame,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockFixture,
) -> None:
    mocker.patch("pyp.cli.plot.commands.growth.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=monthly_prices_df)
    mocker.patch("pyp.cli.plot.commands.growth.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth._monthly_prices_query", mock_property)

    assert monthly_prices_df.equals(command._monthly_prices_df)

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session.bind)


def test_add_monthly_market_prices(
    command: PlotGrowth, monthly_prices_df: DataFrame, monthly_df: DataFrame, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.commands.growth.PlotGrowth._monthly_prices_df", monthly_prices_df)

    command._df = monthly_df

    expected_df = monthly_prices_df.merge(
        monthly_df, how="left", left_on=["moniker", "month"], right_on=["moniker", "month"]
    )

    expected_df[["amount", "invested"]] = expected_df[["amount", "invested"]].fillna(0, axis="columns")
    expected_df["amount"] = expected_df.groupby("moniker")["amount"].cumsum()

    assert command == command._add_monthly_market_prices()

    assert isinstance(command._df, DataFrame)
    assert expected_df.equals(command._df)


def test_compute_market_value(command: PlotGrowth, added_monthly_market_prices_df: DataFrame) -> None:
    command._df = added_monthly_market_prices_df

    df = added_monthly_market_prices_df.copy(deep=True)
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
    df["cum_sum_invested"] = df["invested"].cumsum()
    df = df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

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

    df = profit_ratio_df[["month", "profit_ratio"]].copy(deep=True)
    df["profit_ratio"] = df["profit_ratio"] * 100

    actual_df = command._month_vs_profit_ratio_df

    assert isinstance(actual_df, DataFrame)
    assert df.equals(actual_df)


def test_prepare_df(command: PlotGrowth, mocker: MockFixture) -> None:
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
    mock_subm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_sum_up_by_month", mock_subm)
    mock_ccs = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_cumulative_sums", mock_ccs)
    mock_cp = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit", mock_cp)
    mock_cpr = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_profit_ratio", mock_cpr)

    command._prepare_df()

    mock_rdb.assert_called_once()
    mock_csv.assert_called_once()
    mock_rvti.assert_called_once()
    mock_submam.assert_called_once()
    mock_ammp.assert_called_once()
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
        call(
            x="month",
            stacked=False,
            title="Invested vs. Market",
            xlabel="Month",
            ylabel="Dollars ($)",
            grid=True,
            rot=90,
        ),
        call(
            x="month",
            stacked=False,
            title="Profit Growth",
            xlabel="Month",
            ylabel="Dollars ($)",
            grid=True,
            rot=90,
        ),
    ])
    mock_plot.bar.assert_called_once_with(
        x="month",
        title="Profit Ratio",
        xlabel="Month",
        ylabel="Ratio",
        grid=True,
        rot=90,
    )
    mock_show.assert_called_once()
