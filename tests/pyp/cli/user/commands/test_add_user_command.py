from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.exc import IntegrityError

from pyp.cli.protocols import CommandProtocol
from pyp.cli.user.commands import AddUserCommand
from pyp.database.models import User


@pytest.fixture
def command(mock_engine: MagicMock) -> AddUserCommand:
    return AddUserCommand(mock_engine, "MrSir")


def test_initialization(mock_engine: MagicMock) -> None:
    username = "MrSir"
    command = AddUserCommand(mock_engine, username)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert username == command.username


def test_prepare_user(command: AddUserCommand) -> None:
    command._prepare_user()

    assert isinstance(command._user, User)
    assert command.username == command._user.username


def test_execute(
    command: AddUserCommand, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.user.commands.Session", mock_session_class)

    mock_prepare_user = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_user", mock_prepare_user)

    user = User(username="MrSir")
    command._user = user

    command.execute()

    mock_prepare_user.assert_called_once()
    mock_session.add.assert_called_once_with(user)
    mock_session.commit.assert_called_once()


def test_execute_shows_error_when_user_exists(
    command: AddUserCommand, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockerFixture
) -> None:
    mocker.patch("pyp.cli.user.commands.Session", mock_session_class)

    mock_session.commit = mocker.MagicMock(side_effect=IntegrityError(None, None, Exception()))

    mock_prepare_user = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_user", mock_prepare_user)

    mock_print = mocker.MagicMock()
    mocker.patch("pyp.cli.user.commands.print", mock_print)

    username = "MrSir"
    user = User(username=username)
    command._user = user

    command.execute()

    mock_prepare_user.assert_called_once()
    mock_session.add.assert_called_once_with(user)
    mock_session.commit.assert_called_once()
    mock_print.assert_called_once_with(f"The user '{username}' already exists.")
