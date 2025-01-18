from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from pyp.cli.ingest.commands.base import IngestBaseCommand
from pyp.database.models import Currency


@pytest.fixture
def command(mock_engine: MagicMock) -> IngestBaseCommand:
    return IngestBaseCommand(mock_engine)


def test_initialization(mock_engine: MagicMock) -> None:
    command = IngestBaseCommand(mock_engine)

    assert mock_engine == command.engine


def test_resolve_currencies(
    command: IngestBaseCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    currencies = [Currency(code="USD"), Currency(code="CAD")]

    mocker.patch("pyp.cli.ingest.commands.base.Session", mock_session_class)
    mocker.patch("pyp.cli.ingest.commands.base.select", mock_select)
    mock_session.scalars.return_value.all.return_value = currencies

    command._resolve_currencies()

    assert {c.code: c for c in currencies} == command._currencies_by_code

    mock_session_class.assert_called_once_with(command.engine)
    mock_session.scalars.assert_called_once()
    mock_select.assert_called_once_with(Currency)
