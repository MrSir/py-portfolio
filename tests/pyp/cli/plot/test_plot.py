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


@pytest.fixture
def portfolio_id() -> int:
    return 1


@pytest.fixture
def mock_resolve_portfolio(portfolio_id: int, mocker: MockFixture) -> None:
    mock_resolve_portfolio = mocker.MagicMock(return_value=Portfolio(id=portfolio_id))

    return mock_resolve_portfolio


def test_plot_app(app: Typer) -> None:
    assert "plot" == app.info.name
    assert "Plot various charts of the portfolio." == app.info.help


def test_plot_app_breakdown(app: Typer) -> None:
    assert "breakdown" in [c.name for c in app.registered_commands]

    breakdown = next(iter([c for c in app.registered_commands if c.name == "breakdown"]))

    assert "Plot pie-charts of the portfolio breakdown." == breakdown.help


def test_plot_app_growth(app: Typer) -> None:
    assert "growth" in [c.name for c in app.registered_commands]

    growth = next(iter([c for c in app.registered_commands if c.name == "growth"]))

    assert "Plot charts showing the overall portfolio growth." == growth.help


def test_plot_app_growth_breakdown(app: Typer) -> None:
    assert "growth-breakdown" in [c.name for c in app.registered_commands]

    growth = next(iter([c for c in app.registered_commands if c.name == "growth-breakdown"]))

    assert "Plot charts showing the portfolio growth breakdown." == growth.help


def test_breakdown_command(
    app: Typer,
    cli_runner: CliRunner,
    portfolio_id: int,
    mock_resolve_portfolio: MagicMock,
    mocker: MockFixture,
) -> None:
    mocker.patch("pyp.cli.plot.resolve_portfolio", mock_resolve_portfolio)

    mock_plot_breakdown = mocker.MagicMock()
    mock_plot_breakdown.plot = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=PlotBreakdown, return_value=mock_plot_breakdown)
    mocker.patch("pyp.cli.plot.PlotBreakdown", mock_class)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "breakdown"])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(portfolio_id, output_dir=None)
    mock_plot_breakdown.plot.assert_called_once()


def test_growth_command(
    app: Typer, cli_runner: CliRunner, portfolio_id: int, mock_resolve_portfolio: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.resolve_portfolio", mock_resolve_portfolio)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "growth"])

    assert result.exit_code == 0


def test_growth_breakdown_command(
    app: Typer, cli_runner: CliRunner, portfolio_id: int, mock_resolve_portfolio: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.resolve_portfolio", mock_resolve_portfolio)

    result = cli_runner.invoke(app, ["Username", "PortfolioName", "growth-breakdown"])

    assert result.exit_code == 0
