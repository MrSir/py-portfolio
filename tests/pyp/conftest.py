from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from pyp.database.models import Portfolio


@pytest.fixture
def mock_engine(mocker: MockerFixture) -> MagicMock:
    mock = mocker.MagicMock(spec=Engine)

    return mock


@pytest.fixture
def mock_select(mocker: MockerFixture) -> MagicMock:
    mock_query = mocker.MagicMock()
    mock_query.where = mocker.MagicMock(return_value=mock_query)

    mock_select = mocker.MagicMock(return_value=mock_query)

    return mock_select()


@pytest.fixture
def mock_session(mocker: MockerFixture) -> MagicMock:
    mock_scalars_result = mocker.MagicMock()
    mock_scalars_result.one = mocker.MagicMock()
    mock_scalars_result.first = mocker.MagicMock()

    mock_scalars = mocker.MagicMock(return_value=mock_scalars_result)

    mock_session = mocker.MagicMock()
    mock_session.bind = mocker.MagicMock()
    mock_session.scalars = mock_scalars
    mock_session.add = mocker.MagicMock()
    mock_session.execute = mocker.MagicMock()
    mock_session.commit = mocker.MagicMock()

    return mock_session


@pytest.fixture
def mock_session_class(mock_session: MagicMock, mocker: MockerFixture) -> MagicMock:
    mock_session_class = mocker.MagicMock(spec=Session)
    mock_session_class.return_value.__enter__.return_value = mock_session

    return mock_session_class


@pytest.fixture
def portfolio_id() -> int:
    return 1


@pytest.fixture
def portfolio(portfolio_id: int) -> Portfolio:
    return Portfolio(id=portfolio_id, name="My Portfolio")


@pytest.fixture
def mock_resolve_portfolio(portfolio: Portfolio, mocker: MockerFixture) -> None:
    mock_resolve_portfolio = mocker.MagicMock(return_value=portfolio)

    return mock_resolve_portfolio
