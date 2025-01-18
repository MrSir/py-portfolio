from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from pyp.cli.common import resolve_portfolio
from pyp.database.models import Portfolio


def test_resolve_portfolio(
    mock_session_class: MagicMock, mock_session: MagicMock, mock_select: MagicMock, mocker: MockerFixture
) -> None:
    mock_portfolio = mocker.MagicMock(spec=Portfolio)

    mock_scalars_result = mocker.MagicMock()
    mock_scalars_result.one = mocker.MagicMock(return_value=mock_portfolio)

    mock_scalars = mocker.MagicMock(return_value=mock_scalars_result)
    mock_session.scalars = mock_scalars

    mocker.patch("pyp.cli.common.Session", mock_session_class)
    mocker.patch("pyp.cli.common.select", mock_select)

    username = "MrSir"
    portfolio_name = "My Portfolio"

    assert mock_portfolio == resolve_portfolio(username, portfolio_name)

    mock_select.assert_called_once_with(Portfolio)
    mock_scalars.assert_called_once_with(mock_select.return_value.where.return_value.where.return_value)
    mock_scalars_result.one.assert_called_once()
