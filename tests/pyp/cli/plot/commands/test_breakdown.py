import json
from datetime import datetime
from pathlib import Path
from unittest.mock import call

import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable

from pyp.cli.plot.commands.base import PlotCommand
from pyp.cli.plot.commands.breakdown import PlotBreakdown
from pyp.cli.protocols import PlotCommandProtocol


@pytest.fixture
def command(portfolio_id: int) -> PlotBreakdown:
    return PlotBreakdown(portfolio_id, datetime(2024, 12, 3), output_dir=Path(__file__).parent)


@pytest.fixture
def db_data_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "BTCO", "PLTR", "IYK", "RDVY"],
            "stock_type": ["EQUITY", "ETF", "EQUITY", "ETF", "ETF"],
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


def test_initialization(portfolio_id: int) -> None:
    output_dir = Path(__file__).parent
    date = datetime(2024, 12, 9)
    command = PlotBreakdown(portfolio_id, date, output_dir=output_dir)

    assert isinstance(command, PlotCommand)
    assert isinstance(command, PlotCommandProtocol)


def test_db_query_property(command: PlotBreakdown) -> None:
    db_query = command._db_query

    assert isinstance(db_query, Selectable)

    query = """SELECT DISTINCT
        stocks.moniker,
        stocks.stock_type,
        stocks.sector_weightings,
        sum(shares.amount) AS amount,
        prices.amount AS price
    FROM shares
    JOIN portfolio_stocks ON portfolio_stocks.id = shares.portfolio_stocks_id
    JOIN stocks ON stocks.id = portfolio_stocks.stock_id
    JOIN prices ON stocks.id = prices.stock_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1
        AND prices.date = :date_1 GROUP BY shares.portfolio_stocks_id"""

    expected_query = query.replace("\n        ", " ").replace("\n    ", " ")

    assert expected_query == str(db_query).replace("\n", "")


def test_df_dtypes_property(command: PlotBreakdown) -> None:
    assert {
        "moniker": "string",
        "stock_type": "string",
        "sector_weightings": "object",
        "amount": "float64",
        "price": "float64",
    } == command._df_dtypes


def test_expand_by_sector(command: PlotBreakdown, share_value_df: DataFrame) -> None:
    command._df = share_value_df

    df = share_value_df
    df["sector_weightings"] = df["sector_weightings"].str.replace("_", "-")

    df = df.join(DataFrame(df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0)).drop(
        columns=["amount", "sector_weightings"]
    )

    assert command == command._expand_by_sector()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_percent_by_moniker(command: PlotBreakdown, expand_by_sector_df: DataFrame) -> None:
    command._df = expand_by_sector_df

    df = expand_by_sector_df
    df["total_value"] = df["value"].sum()
    df["percent"] = df["value"] / df["total_value"]
    df = df.drop(columns=["value", "total_value"])

    assert command == command._calculate_percent_by_moniker()

    assert isinstance(command._df, DataFrame)
    assert df.equals(command._df)


def test_moniker_breakdown_df_property(command: PlotBreakdown, percent_by_moniker_df: DataFrame) -> None:
    command._df = percent_by_moniker_df

    df = percent_by_moniker_df[["moniker", "percent"]]

    percent_by_moniker_df = command._moniker_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_stock_type_breakdown_df_property(command: PlotBreakdown, percent_by_moniker_df: DataFrame) -> None:
    command._df = percent_by_moniker_df

    df = percent_by_moniker_df[["stock_type", "percent"]].groupby("stock_type").sum().reset_index()

    percent_by_moniker_df = command._stock_type_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_sector_breakdown_df_property(command: PlotBreakdown, percent_by_moniker_df: DataFrame) -> None:
    command._df = percent_by_moniker_df

    df = percent_by_moniker_df.drop(columns=["moniker", "stock_type"])
    df_minus_percent = df.drop(columns="percent")
    df = df_minus_percent.multiply(df["percent"], axis="index")
    df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

    percent_by_moniker_df = command._sector_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_prepare_df(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_rdb = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_read_db", mock_rdb)
    mock_csv = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_compute_share_value", mock_csv)
    mock_ebs = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_expand_by_sector", mock_ebs)
    mock_cpbm = mocker.MagicMock(return_value=command)
    mocker.patch.object(command, "_calculate_percent_by_moniker", mock_cpbm)

    command._prepare_df()

    mock_rdb.assert_called_once()
    mock_csv.assert_called_once()
    mock_ebs.assert_called_once()
    mock_cpbm.assert_called_once()


def test_writes_json_files(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_moniker_breakdown_df = mocker.MagicMock()
    mock_moniker_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown._moniker_breakdown_df", mock_moniker_breakdown_df)
    mock_stock_type_breakdown_df = mocker.MagicMock()
    mock_stock_type_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown._stock_type_breakdown_df", mock_stock_type_breakdown_df)
    mock_sector_breakdown_df = mocker.MagicMock()
    mock_sector_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown._sector_breakdown_df", mock_sector_breakdown_df)

    command._write_json_files()
    assert command.output_dir is not None

    mock_moniker_breakdown_df.to_json.assert_called_once_with(
        command.output_dir / "moniker_breakdown.json", orient="records"
    )
    mock_stock_type_breakdown_df.to_json.assert_called_once_with(
        command.output_dir / "stock_type_breakdown.json", orient="records"
    )
    mock_sector_breakdown_df.to_json.assert_called_once_with(
        command.output_dir / "sector_breakdown.json", orient="records"
    )


def test_show(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_plot = mocker.MagicMock()
    mock_plot.pie = mocker.MagicMock()
    mock_moniker_breakdown_df = mocker.MagicMock()
    mock_moniker_breakdown_df.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown._moniker_breakdown_df", mock_moniker_breakdown_df)
    mock_stock_type_breakdown_df = mocker.MagicMock()
    mock_stock_type_breakdown_df.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown._stock_type_breakdown_df", mock_stock_type_breakdown_df)
    mock_sector_breakdown_df = mocker.MagicMock()
    mock_sector_breakdown_df.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown._sector_breakdown_df", mock_sector_breakdown_df)

    mock_show = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.plt.show", mock_show)

    command._show()

    mock_plot.pie.assert_has_calls([
        call(
            title="Percent of Portfolio by Moniker",
            y="percent",
            labels=mock_moniker_breakdown_df["moniker"],
            figsize=(5, 5),
            autopct="%.2f%%",
            legend=False,
        ),
        call(
            title="Percent of Portfolio by Stock Type",
            y="percent",
            labels=mock_stock_type_breakdown_df["stock_type"],
            figsize=(5, 5),
            autopct="%.2f%%",
            legend=False,
        ),
        call(
            title="Percent of Portfolio by Sector",
            y="percent",
            labels=mock_sector_breakdown_df["sector"],
            figsize=(5, 5),
            autopct="%.2f%%",
            legend=False,
        ),
    ])
    mock_show.assert_called_once()
