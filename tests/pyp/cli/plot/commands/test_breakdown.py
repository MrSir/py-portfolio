from unittest.mock import MagicMock

import pytest
from pandas import DataFrame
from pytest_mock import MockFixture
from sqlalchemy import Selectable

from pyp.cli.plot.commands.breakdown import PlotBreakdown
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


def test_db_data_df_property(command: PlotBreakdown) -> None:
    expected_df = DataFrame()

    assert expected_df.equals(command.db_data_df)
