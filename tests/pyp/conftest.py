from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture
from sqlalchemy.orm import Session


@pytest.fixture
def mock_session(mocker: MockFixture) -> MagicMock:
    mock_scalars = mocker.MagicMock()
    mock_scalars.one = mocker.MagicMock()

    mock_session = mocker.MagicMock()
    mock_session.bind = mocker.MagicMock()
    mock_session.scalars = mock_scalars

    return mock_session


@pytest.fixture
def mock_session_class(mock_session: MagicMock, mocker: MockFixture) -> MagicMock:
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session

    return mock_session_class
