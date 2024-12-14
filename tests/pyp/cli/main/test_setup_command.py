from unittest.mock import MagicMock, call

import pytest
from pytest_mock import MockFixture
from sqlalchemy import Engine

from pyp.cli.commands import SetupCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Currency


@pytest.fixture
def mock_engine(mocker: MockFixture) -> MagicMock:
    mock = mocker.MagicMock(spec=Engine)

    return mock


@pytest.fixture
def command(mock_engine: MagicMock) -> SetupCommand:
    return SetupCommand(mock_engine)


@pytest.fixture
def default_currencies() -> list[Currency]:
    currencies = [
        {"name": "USD", "full_name": "United States Dollar"},
        {"name": "CAD", "full_name": "Canadian Dollar"},
    ]

    return [Currency(**c) for c in currencies]


def test_initialization(mock_engine: MagicMock) -> None:
    seed = True

    command = SetupCommand(mock_engine, seed=seed)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert seed == command.seed


def test_create_schema(command: SetupCommand, mocker: MockFixture) -> None:
    mock_create_all = mocker.MagicMock()
    mocker.patch("pyp.cli.commands.Base.metadata.create_all", mock_create_all)

    assert command == command._create_schema()

    mock_create_all.assert_called_once_with(command.engine)


def test_default_currencies(command: SetupCommand) -> None:
    currencies = [
        {"name": "USD", "full_name": "United States Dollar"},
        {"name": "CAD", "full_name": "Canadian Dollar"},
    ]

    assert currencies == [{"name": c.name, "full_name": c.full_name} for c in command._default_currencies]


def test_seed_db(
    command: SetupCommand,
    default_currencies: list[Currency],
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockFixture,
) -> None:
    mocker.patch("pyp.cli.commands.Session", mock_session_class)

    mocker.patch("pyp.cli.commands.SetupCommand._default_currencies", default_currencies)

    assert command == command._seed_db()

    mock_session.add.assert_has_calls([call(c) for c in default_currencies])
    mock_session.commit.assert_called_once()


def test_execute_without_seed(command: SetupCommand, mocker: MockFixture) -> None:
    mock_create_schema = mocker.MagicMock()
    mocker.patch.object(command, "_create_schema", mock_create_schema)
    mock_seed_db = mocker.MagicMock()
    mocker.patch.object(command, "_seed_db", mock_seed_db)

    command.seed = False
    command.execute()

    mock_create_schema.assert_called_once()
    mock_seed_db.assert_not_called()


def test_execute_with_seed(command: SetupCommand, mocker: MockFixture) -> None:
    mock_create_schema = mocker.MagicMock()
    mocker.patch.object(command, "_create_schema", mock_create_schema)
    mock_seed_db = mocker.MagicMock()
    mocker.patch.object(command, "_seed_db", mock_seed_db)

    command.seed = True
    command.execute()

    mock_create_schema.assert_called_once()
    mock_seed_db.assert_called_once()
