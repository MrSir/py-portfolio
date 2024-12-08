import json
from pathlib import Path
from unittest.mock import call

import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable
from sqlalchemy.orm import Session

from pyp.cli.plot.commands.breakdown import PlotBreakdown
from pyp.database.engine import engine


@pytest.fixture
def command() -> PlotBreakdown:
    return PlotBreakdown(1, output_dir=Path(__file__).parent)


@pytest.fixture
def db_data_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "SMH", "ADP", "VGT", "SMH", "SMH"],
            "stock_type": ["EQUITY", "ETF", "EQUITY", "ETF", "ETF", "ETF"],
            "sector_weightings": [
                json.dumps({"financials": 1.0}),
                json.dumps({"technology": 1.0}),
                json.dumps({"financials": 1.0}),
                json.dumps({"technology": 1.0}),
                json.dumps({"technology": 1.0}),
                json.dumps({"technology": 1.0}),
            ],
            "amount": [1.23, 3.4, 0.234, 5.5, 12.3, 0.7],
            "price": [65.324, 23.566, 123.54, 355.0, 123.32, 120.23],
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

    return df.drop(columns=["amount", "price"])


@pytest.fixture
def group_by_moniker_df(share_value_df: DataFrame) -> DataFrame:
    return share_value_df.groupby(["moniker", "stock_type", "sector_weightings"]).sum().reset_index()


@pytest.fixture
def expand_by_sector_df(group_by_moniker_df: DataFrame) -> DataFrame:
    df = group_by_moniker_df.copy(deep=True)
    df = df.join(DataFrame(group_by_moniker_df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))
    return df.drop(columns=["sector_weightings"])


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


def test_initialization() -> None:
    output_dir = Path(__file__).parent
    command = PlotBreakdown(1, output_dir=output_dir)

    assert 1 == command.portfolio_id
    assert output_dir == command.output_dir
    assert command.show is False


def test_db_query_property(command: PlotBreakdown) -> None:
    db_query = command.db_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        stocks.stock_type,
        stocks.sector_weightings,
        shares.amount,
        shares.price
    FROM shares
    JOIN portfolio_stocks ON portfolio_stocks.id = shares.portfolio_stocks_id
    JOIN stocks ON stocks.id = portfolio_stocks.stock_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1"""

    expected_query = query.replace("\n        ", " ").replace("\n    ", " ")

    assert expected_query == str(db_query).replace("\n", "")


def test_db_data_df_property(command: PlotBreakdown, db_data_df: DataFrame, mocker: MockFixture) -> None:
    mock_session_bind = mocker.MagicMock()
    mock_session = mocker.MagicMock()
    mock_session.bind = mock_session_bind
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session
    mocker.patch("pyp.cli.plot.commands.breakdown.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.db_query", mock_property)

    assert isinstance(command.db_data_df, DataFrame)

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session_bind)


def test_db_data_df_property_caches(command: PlotBreakdown, db_data_df: DataFrame, mocker: MockFixture) -> None:
    mock_session_bind = mocker.MagicMock()
    mock_session = mocker.MagicMock()
    mock_session.bind = mock_session_bind
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session
    mocker.patch("pyp.cli.plot.commands.breakdown.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.db_query", mock_property)

    assert isinstance(command.db_data_df, DataFrame), "First time it computes"
    assert isinstance(command.db_data_df, DataFrame), "Second time it caches"

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session_bind)


def test_share_value_df_property(command: PlotBreakdown, db_data_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.db_data_df", mock_property)

    df = db_data_df.copy(deep=True)
    df["value"] = df["amount"] * df["price"]
    df = df.drop(columns=["amount", "price"])

    share_value_df = command.share_value_df

    assert isinstance(share_value_df, DataFrame)
    assert df.equals(share_value_df)


def test_group_by_moniker_df_property(command: PlotBreakdown, share_value_df: DataFrame, mocker: MockFixture) -> None:
    mock_property = mocker.PropertyMock(return_value=share_value_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.share_value_df", mock_property)

    df = share_value_df.groupby(["moniker", "stock_type", "sector_weightings"]).sum().reset_index()

    group_by_moniker_df = command.group_by_moniker_df

    assert isinstance(group_by_moniker_df, DataFrame)
    assert df.equals(group_by_moniker_df)


def test_expand_by_sector_df_property(
    command: PlotBreakdown, group_by_moniker_df: DataFrame, mocker: MockFixture
) -> None:
    mock_property = mocker.PropertyMock(return_value=group_by_moniker_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.group_by_moniker_df", mock_property)

    df = group_by_moniker_df.copy(deep=True)
    df = df.join(DataFrame(group_by_moniker_df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))
    df = df.drop(columns=["sector_weightings"])

    expand_by_sector_df = command.expand_by_sector_df

    assert isinstance(expand_by_sector_df, DataFrame)
    assert df.equals(expand_by_sector_df)


def test_percent_by_moniker_df_property(
    command: PlotBreakdown, expand_by_sector_df: DataFrame, mocker: MockFixture
) -> None:
    mock_property = mocker.PropertyMock(return_value=expand_by_sector_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.expand_by_sector_df", mock_property)

    df = expand_by_sector_df.copy(deep=True)
    df["total_value"] = df["value"].sum()
    df["percent"] = df["value"] / df["total_value"]
    df = df.drop(columns=["value", "total_value"])

    percent_by_moniker_df = command.percent_by_moniker_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_moniker_breakdown_df_property(
    command: PlotBreakdown, percent_by_moniker_df: DataFrame, mocker: MockFixture
) -> None:
    mock_property = mocker.PropertyMock(return_value=percent_by_moniker_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.percent_by_moniker_df", mock_property)

    df = percent_by_moniker_df.copy(deep=True)
    df = df[["moniker", "percent"]]

    percent_by_moniker_df = command.moniker_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_stock_type_breakdown_df_property(
    command: PlotBreakdown, percent_by_moniker_df: DataFrame, mocker: MockFixture
) -> None:
    mock_property = mocker.PropertyMock(return_value=percent_by_moniker_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.percent_by_moniker_df", mock_property)

    df = percent_by_moniker_df.copy(deep=True)
    df = df[["stock_type", "percent"]].groupby("stock_type").sum().reset_index()

    percent_by_moniker_df = command.stock_type_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_sector_breakdown_df_property(
    command: PlotBreakdown, percent_by_moniker_df: DataFrame, mocker: MockFixture
) -> None:
    mock_property = mocker.PropertyMock(return_value=percent_by_moniker_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.percent_by_moniker_df", mock_property)

    df = percent_by_moniker_df.copy(deep=True)
    df = df.drop(columns=["moniker", "stock_type"])
    df_minus_percent = df.drop(columns="percent")
    df = df_minus_percent.multiply(df["percent"], axis="index")
    df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

    percent_by_moniker_df = command.sector_breakdown_df

    assert isinstance(percent_by_moniker_df, DataFrame)
    assert df.equals(percent_by_moniker_df)


def test_writes_json_files(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_moniker_breakdown_df = mocker.MagicMock()
    mock_moniker_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.moniker_breakdown_df", mock_moniker_breakdown_df)
    mock_stock_type_breakdown_df = mocker.MagicMock()
    mock_stock_type_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.stock_type_breakdown_df", mock_stock_type_breakdown_df)
    mock_sector_breakdown_df = mocker.MagicMock()
    mock_sector_breakdown_df.to_json = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.sector_breakdown_df", mock_sector_breakdown_df)

    command.write_json_files()
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


def test_show_breakdowns(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_plot = mocker.MagicMock()
    mock_plot.pie = mocker.MagicMock()
    mock_moniker_breakdown_df = mocker.MagicMock()
    mock_moniker_breakdown_df.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.moniker_breakdown_df", mock_moniker_breakdown_df)
    mock_stock_type_breakdown_df = mocker.MagicMock()
    mock_stock_type_breakdown_df.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.stock_type_breakdown_df", mock_stock_type_breakdown_df)
    mock_sector_breakdown_df = mocker.MagicMock()
    mock_sector_breakdown_df.plot = mock_plot
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.sector_breakdown_df", mock_sector_breakdown_df)

    mock_show = mocker.MagicMock()
    mocker.patch("pyp.cli.plot.commands.breakdown.plt.show", mock_show)

    command.show_breakdowns()

    mock_plot.pie.assert_has_calls([
        call(y="percent", labels=mock_moniker_breakdown_df["moniker"], figsize=(5, 5), autopct="%.2f%%"),
        call(y="percent", labels=mock_stock_type_breakdown_df["stock_type"], figsize=(5, 5), autopct="%.2f%%"),
        call(y="percent", labels=mock_sector_breakdown_df["sector"], figsize=(5, 5), autopct="%.2f%%"),
    ])
    mock_show.assert_called_once()


def test_plot_writes_json_files_when_output_dir_is_provided(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_write_json_files = mocker.MagicMock()
    mocker.patch.object(command, "write_json_files", mock_write_json_files)

    command.plot()

    mock_write_json_files.assert_called_once()


def test_plot_shows_breakdowns_if_show_flag(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_show_breakdowns = mocker.MagicMock()
    mocker.patch.object(command, "show_breakdowns", mock_show_breakdowns)
    command.output_dir = None

    command.plot()

    mock_show_breakdowns.assert_called_once()
