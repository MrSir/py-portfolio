from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pyp.cli.portfolio.commands import AddMonikerCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Portfolio, Stock


@pytest.fixture
def command(mock_engine: MagicMock) -> AddMonikerCommand:
    return AddMonikerCommand(mock_engine, Portfolio(name="My Portfolio"), "QQQ")


def test_initialization(mock_engine: MagicMock) -> None:
    portfolio = Portfolio(name="My Portfolio")
    moniker = "QQQ"
    command = AddMonikerCommand(mock_engine, portfolio, moniker)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert portfolio == command.portfolio
    assert moniker == command.moniker

    assert command._stock is None


def test_resolve_stock_finds_and_returns_stock(
    command: AddMonikerCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    expected_stock = Stock(moniker=command.moniker)

    mocker.patch("pyp.cli.portfolio.commands.Session", mock_session_class)
    mocker.patch("pyp.cli.portfolio.commands.select", mock_select)
    mock_session.scalars.return_value.first.return_value = expected_stock

    stock = command._resolve_stock()

    assert isinstance(stock, Stock)
    assert command.moniker == stock.moniker

    mock_select.assert_called_once_with(Stock)
    mock_select.return_value.where.assert_called_once()
    mock_session.scalars.assert_called_once_with(mock_select.return_value.where.return_value)
    mock_session.scalars.return_value.first.assert_called_once()


def test_resolve_stock_returns_none(
    command: AddMonikerCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.portfolio.commands.Session", mock_session_class)
    mocker.patch("pyp.cli.portfolio.commands.select", mock_select)
    mock_session.scalars.return_value.first.return_value = None

    assert command._resolve_stock() is None

    mock_select.assert_called_once_with(Stock)
    mock_select.return_value.where.assert_called_once()
    mock_session.scalars.assert_called_once_with(mock_select.return_value.where.return_value)
    mock_session.scalars.return_value.first.assert_called_once()


def test_create_stock(
    command: AddMonikerCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    expected_stock = Stock(moniker=command.moniker)

    mocker.patch("pyp.cli.portfolio.commands.Session", mock_session_class)
    mock_class = mocker.MagicMock(spec=Stock, return_value=expected_stock)
    mocker.patch("pyp.cli.portfolio.commands.Stock", mock_class)

    stock = command._create_stock()

    assert isinstance(stock, Stock)
    assert command.moniker == stock.moniker

    mock_class.assert_called_once_with(moniker=command.moniker)
    mock_session.add.assert_called_once_with(expected_stock)
    mock_session.commit.assert_called_once()


def test_stock_property_returns_set_value(command: AddMonikerCommand) -> None:
    stock = Stock(moniker=command.moniker)

    command._stock = stock

    assert stock == command.stock


def test_stock_property_resolves_if_not_set(command: AddMonikerCommand, mocker: MockerFixture) -> None:
    expected_stock = Stock(moniker=command.moniker)

    mock_resolve_stock = mocker.MagicMock(return_value=expected_stock)
    mocker.patch.object(command, "_resolve_stock", mock_resolve_stock)
    mock_create_stock = mocker.MagicMock()
    mocker.patch.object(command, "_create_stock", mock_create_stock)

    assert expected_stock == command.stock

    mock_resolve_stock.assert_called_once()
    mock_create_stock.assert_not_called()


def test_stock_property_creates_if_not_resolvable(command: AddMonikerCommand, mocker: MockerFixture) -> None:
    expected_stock = Stock(moniker=command.moniker)

    mock_resolve_stock = mocker.MagicMock(return_value=None)
    mocker.patch.object(command, "_resolve_stock", mock_resolve_stock)
    mock_create_stock = mocker.MagicMock(return_value=expected_stock)
    mocker.patch.object(command, "_create_stock", mock_create_stock)

    assert expected_stock == command.stock

    mock_resolve_stock.assert_called_once()
    mock_create_stock.assert_called_once()


def test_execute(
    command: AddMonikerCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mock_stocks_relationship = mocker.MagicMock()
    mock_stocks_relationship.append = mocker.MagicMock()
    mock_portfolio = mocker.MagicMock(spec=Portfolio)
    mock_portfolio.stocks = mock_stocks_relationship
    stock = Stock(moniker=command.moniker)

    command.portfolio = mock_portfolio

    mocker.patch("pyp.cli.portfolio.commands.Session", mock_session_class)
    mock_property = mocker.PropertyMock(return_value=stock)
    mocker.patch("pyp.cli.portfolio.commands.AddMonikerCommand.stock", mock_property)

    command.execute()

    mock_session.add.assert_called_once_with(mock_portfolio)
    mock_portfolio.stocks.append.assert_called_once_with(stock)
    mock_session.commit.assert_called_once()
