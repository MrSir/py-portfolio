from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.exc import IntegrityError

from pyp.cli.protocols import CommandProtocol
from pyp.cli.user.commands import AddPortfolioCommand
from pyp.database.models import Portfolio, User


@pytest.fixture
def command(mock_engine: MagicMock) -> AddPortfolioCommand:
    return AddPortfolioCommand(mock_engine, "MrSir", "My Portfolio")


def test_initialization(mock_engine: MagicMock) -> None:
    username = "MrSir"
    name = "My Portfolio"
    command = AddPortfolioCommand(mock_engine, username, name)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert username == command.username
    assert name == command.name


def test_prepare_portfolio(command: AddPortfolioCommand) -> None:
    command._prepare_portfolio()

    assert isinstance(command._portfolio, Portfolio)
    assert command.name == command._portfolio.name


def test_resolve_user(
    command: AddPortfolioCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    user = User(username=command.username)

    mocker.patch("pyp.cli.user.commands.Session", mock_session_class)
    mocker.patch("pyp.cli.user.commands.select", mock_select)
    mock_session.scalars.return_value.one.return_value = user

    command._resolve_user()

    assert isinstance(command._user, User)
    assert command.username == command._user.username

    mock_select.assert_called_once_with(User)
    mock_select.return_value.where.assert_called_once()
    mock_session.scalars.assert_called_once_with(mock_select.return_value.where.return_value)
    mock_session.scalars.return_value.one.assert_called_once()


def test_execute(
    command: AddPortfolioCommand, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.user.commands.Session", mock_session_class)

    mock_prepare_portfolio = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_portfolio", mock_prepare_portfolio)
    mock_resolve_user = mocker.MagicMock()
    mocker.patch.object(command, "_resolve_user", mock_resolve_user)

    user = mocker.MagicMock(spec=User)
    user.portfolios = mocker.PropertyMock()
    portfolio = Portfolio(name="My Portfolio")
    command._user = user
    command._portfolio = portfolio

    command.execute()

    mock_prepare_portfolio.assert_called_once()
    mock_resolve_user.assert_called_once()
    mock_session.add.assert_called_once_with(user)
    user.portfolios.append.assert_called_once_with(portfolio)
    mock_session.commit.assert_called_once()


def test_execute_shows_error_when_user_exists(
    command: AddPortfolioCommand, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.user.commands.Session", mock_session_class)

    mock_session.commit = mocker.MagicMock(side_effect=IntegrityError(None, None, Exception()))

    mock_prepare_portfolio = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_portfolio", mock_prepare_portfolio)
    mock_resolve_user = mocker.MagicMock()
    mocker.patch.object(command, "_resolve_user", mock_resolve_user)

    mock_print = mocker.MagicMock()
    mocker.patch("pyp.cli.user.commands.print", mock_print)

    username = command.username
    name = command.name
    user = mocker.MagicMock(spec=User)
    user.username = username
    user.portfolios = mocker.PropertyMock()
    portfolio = Portfolio(name=name)
    command._user = user
    command._portfolio = portfolio

    command.execute()

    mock_prepare_portfolio.assert_called_once()
    mock_resolve_user.assert_called_once()
    mock_session.add.assert_called_once_with(user)
    user.portfolios.append.assert_called_once_with(portfolio)
    mock_session.commit.assert_called_once()
    mock_print.assert_called_once_with(f"The portfolio '{name}' already exists, for user '{username}'.")
