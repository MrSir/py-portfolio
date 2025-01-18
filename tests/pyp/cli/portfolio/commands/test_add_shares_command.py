from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pyp.cli.portfolio.commands import AddSharesCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Portfolio, PortfolioStocks, Share


@pytest.fixture
def command(mock_engine: MagicMock) -> AddSharesCommand:
    return AddSharesCommand(mock_engine, Portfolio(name="My Portfolio"), "QQQ", 1.2, 123.45, datetime(2024, 12, 18))


def test_initialization(mock_engine: MagicMock) -> None:
    portfolio = Portfolio(name="My Portfolio")
    moniker = "QQQ"
    amount = 1.2
    price = 123.45
    purchased_on = datetime(2024, 12, 18)
    command = AddSharesCommand(mock_engine, portfolio, moniker, amount, price, purchased_on)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert portfolio == command.portfolio
    assert moniker == command.moniker
    assert amount == command.amount
    assert price == command.price
    assert purchased_on == command.purchased_on


def test_resolve_portfolio_stocks(
    command: AddSharesCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    expected_portfolio_stocks = PortfolioStocks()

    mocker.patch("pyp.cli.portfolio.commands.Session", mock_session_class)
    mocker.patch("pyp.cli.portfolio.commands.select", mock_select)
    mock_session.scalars.return_value.one.return_value = expected_portfolio_stocks

    command._resolve_portfolio_stocks()

    assert isinstance(command._portfolio_stocks, PortfolioStocks)
    assert expected_portfolio_stocks == command._portfolio_stocks

    mock_select.assert_called_once_with(PortfolioStocks)
    mock_select.return_value.where.assert_called_once()
    mock_session.scalars.assert_called_once_with(mock_select.return_value.where.return_value.where.return_value)
    mock_session.scalars.return_value.one.assert_called_once()


def test_prepare_share(command: AddSharesCommand) -> None:
    command._prepare_share()

    assert isinstance(command._share, Share)
    assert command.amount == command._share.amount
    assert command.price == command._share.price
    assert command.purchased_on == command._share.purchased_on


def test_execute(
    command: AddSharesCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mock_shares_relationship = mocker.MagicMock()
    mock_shares_relationship.append = mocker.MagicMock()
    mock_portfolio_stocks = mocker.MagicMock(spec=PortfolioStocks)
    mock_portfolio_stocks.shares = mock_shares_relationship

    share = Share()

    command._portfolio_stocks = mock_portfolio_stocks
    command._share = share

    mocker.patch("pyp.cli.portfolio.commands.Session", mock_session_class)
    mock_rps = mocker.MagicMock()
    mocker.patch.object(command, "_resolve_portfolio_stocks", mock_rps)
    mock_ps = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_share", mock_ps)

    command.execute()

    mock_session.add.assert_called_once_with(mock_portfolio_stocks)
    mock_portfolio_stocks.shares.append.assert_called_once_with(share)
    mock_session.commit.assert_called_once()
