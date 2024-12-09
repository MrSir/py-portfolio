from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.plot import PlotBreakdown, plot_app
from pyp.database.models import Portfolio


@pytest.fixture
def app() -> Typer:
    return plot_app


def test_breakdown_command(
    app: Typer, cli_runner: CliRunner, mock_session_class: MagicMock, mock_session: MagicMock, mocker: MockFixture
) -> None:
    portfolio_id = 1
    mock_scalars_result = mocker.MagicMock()
    mock_scalars_result.one = mocker.MagicMock(return_value=Portfolio(id=portfolio_id))
    mock_session.scalars.return_value = mock_scalars_result
    mocker.patch("pyp.cli.plot.Session", mock_session_class)

    mock_plot_breakdown = mocker.MagicMock()
    mock_plot_breakdown.plot = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=PlotBreakdown, return_value=mock_plot_breakdown)
    mocker.patch("pyp.cli.plot.PlotBreakdown", mock_class)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "breakdown"])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(portfolio_id, output_dir=None)
    mock_plot_breakdown.plot.assert_called_once()


def test_growth_command(app: Typer, cli_runner: CliRunner, mock_session_class: MagicMock, mocker: MockFixture) -> None:
    mocker.patch("pyp.cli.plot.Session", mock_session_class)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "growth"])

    assert result.exit_code == 0


def test_growth_breakdown_mom_command(
    app: Typer, cli_runner: CliRunner, mock_session_class: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.Session", mock_session_class)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "growth-breakdown-mom"])

    assert result.exit_code == 0


def test_growth_breakdown_command(
    app: Typer, cli_runner: CliRunner, mock_session_class: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.Session", mock_session_class)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "growth-breakdown"])

    assert result.exit_code == 0
