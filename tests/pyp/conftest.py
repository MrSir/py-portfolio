from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture
from sqlalchemy.orm import Session

from pyp.database.models import Portfolio


@pytest.fixture
def mock_session(mocker: MockFixture) -> MagicMock:
    mock_scalars = mocker.MagicMock()
    mock_scalars.one = mocker.MagicMock()

    mock_session = mocker.MagicMock()
    mock_session.bind = mocker.MagicMock()
    mock_session.scalars = mock_scalars
    mock_session.add = mocker.MagicMock()
    mock_session.commit = mocker.MagicMock()

    return mock_session


@pytest.fixture
def mock_session_class(mock_session: MagicMock, mocker: MockFixture) -> MagicMock:
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session

    return mock_session_class


@pytest.fixture
def portfolio_id() -> int:
    return 1


@pytest.fixture
def portfolio(portfolio_id: int) -> Portfolio:
    return Portfolio(id=portfolio_id)


@pytest.fixture
def mock_resolve_portfolio(portfolio: Portfolio, mocker: MockFixture) -> None:
    mock_resolve_portfolio = mocker.MagicMock(return_value=portfolio)

    return mock_resolve_portfolio
