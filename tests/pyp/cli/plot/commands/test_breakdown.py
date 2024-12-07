from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable
from sqlalchemy.orm import Session

from pyp.cli.plot.commands.breakdown import PlotBreakdown
from pyp.database.engine import engine
from pyp.database.models import Portfolio


@pytest.fixture
def mock_portfolio(mocker: MockFixture) -> MagicMock:
    mock_portfolio = mocker.MagicMock(Portfolio)

    return mock_portfolio


@pytest.fixture
def command(mock_portfolio: MagicMock) -> PlotBreakdown:
    return PlotBreakdown(mock_portfolio)


def test_initialization(mock_portfolio: MagicMock) -> None:
    command = PlotBreakdown(mock_portfolio)

    assert mock_portfolio == command.portfolio


def test_db_query_property(command: PlotBreakdown) -> None:
    db_query = command.db_query

    assert isinstance(db_query, Selectable)

    query = """SELECT
        stocks.moniker,
        stocks.stock_type,
        stocks.sector_weightings,
        shares.amount,
        shares.price,
        shares.purchased_on,
        currencies.name AS currency
    FROM shares
    JOIN portfolio_stocks ON portfolio_stocks.id = shares.portfolio_stocks_id
    JOIN stocks ON stocks.id = portfolio_stocks.stock_id
    JOIN currencies ON currencies.id = stocks.currency_id
    WHERE portfolio_stocks.portfolio_id = :portfolio_id_1"""

    expected_query = query.replace("\n        ", " ").replace("\n    ", " ")

    assert expected_query == str(db_query).replace("\n", "")


def test_db_data_df_property(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_session_bind = mocker.MagicMock()
    mock_session = mocker.MagicMock()
    mock_session.bind = mock_session_bind
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session
    mocker.patch("pyp.cli.plot.commands.breakdown.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=DataFrame())
    mocker.patch("pyp.cli.plot.commands.breakdown.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.db_query", mock_property)

    assert isinstance(command.db_data_df, DataFrame)

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session_bind)


def test_db_data_df_property_caches(command: PlotBreakdown, mocker: MockFixture) -> None:
    mock_session_bind = mocker.MagicMock()
    mock_session = mocker.MagicMock()
    mock_session.bind = mock_session_bind
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session
    mocker.patch("pyp.cli.plot.commands.breakdown.Session", mock_session_class)

    mock_read_sql = mocker.MagicMock(return_value=DataFrame())
    mocker.patch("pyp.cli.plot.commands.breakdown.pd.read_sql", mock_read_sql)

    db_query = Selectable()
    mock_property = mocker.PropertyMock(return_value=db_query)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.db_query", mock_property)

    assert isinstance(command.db_data_df, DataFrame), "First time it computes"
    assert isinstance(command.db_data_df, DataFrame), "Second time it caches"

    mock_session_class.assert_called_once_with(engine)
    mock_read_sql.assert_called_once_with(db_query, mock_session_bind)


def test_shave_value_df_property(command: PlotBreakdown, mocker: MockFixture) -> None:
    db_data_df = DataFrame(
        data={
            "amount": [1.23, 3.4, 0.234],
            "price": [65.324, 23.566, 123.54],
        }
    )
    mock_property = mocker.PropertyMock(return_value=db_data_df)
    mocker.patch("pyp.cli.plot.commands.breakdown.PlotBreakdown.db_data_df", mock_property)

    expected_share_value_df = DataFrame(data={"value": []})
    expected_share_value_df["value"] = db_data_df["amount"] * db_data_df["price"]

    share_value_df = command.share_value_df

    assert isinstance(share_value_df, DataFrame)
    assert expected_share_value_df.equals(share_value_df)
