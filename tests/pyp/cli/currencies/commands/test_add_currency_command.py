from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.exc import IntegrityError

from pyp.cli.currencies.commands import AddCurrencyCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Currency


@pytest.fixture
def command(mock_engine: MagicMock) -> AddCurrencyCommand:
    return AddCurrencyCommand(mock_engine, "USD", "United States Dollar")


def test_initialization(mock_engine: MagicMock) -> None:
    code = "USD"
    name = "United States Dollar"
    command = AddCurrencyCommand(mock_engine, code, name)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert code == command.code
    assert name == command.name

    assert command._currency is None


def test_prepare_currency(command: AddCurrencyCommand) -> None:
    assert command._currency is None

    command._prepare_currency()

    assert isinstance(command._currency, Currency)
    assert command.code == command._currency.code


def test_execute(
    command: AddCurrencyCommand, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.currencies.commands.Session", mock_session_class)

    mock_prepare_currency = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_currency", mock_prepare_currency)

    currency = Currency(code=command.code, name=command.name)
    command._currency = currency

    command.execute()

    mock_prepare_currency.assert_called_once()
    mock_session.add.assert_called_once_with(currency)
    mock_session.commit.assert_called_once()


def test_execute_shows_error_when_user_exists(
    command: AddCurrencyCommand, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.currencies.commands.Session", mock_session_class)

    mock_session.commit = mocker.MagicMock(side_effect=IntegrityError(None, None, Exception()))

    mock_prepare_currency = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_currency", mock_prepare_currency)

    mock_print = mocker.MagicMock()
    mocker.patch("pyp.cli.currencies.commands.print", mock_print)

    currency = Currency(code=command.code, name=command.name)
    command._currency = currency

    command.execute()

    mock_prepare_currency.assert_called_once()
    mock_session.add.assert_called_once_with(currency)
    mock_session.commit.assert_called_once()
    mock_print.assert_called_once_with(f"The currency '{command.code}' ({command.name}) already exists.")
