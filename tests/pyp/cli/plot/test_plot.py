from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.plot import PlotBreakdown, PlotGrowth, PlotGrowthBreakdown, PlotGrowthBreakdownMonthOverMonth, plot_app


@pytest.fixture
def app() -> Typer:
    return plot_app


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
    mock_plot_breakdown.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=PlotBreakdown, return_value=mock_plot_breakdown)
    mocker.patch("pyp.cli.plot.PlotBreakdown", mock_class)

    date = datetime(2024, 12, 9)

    result = cli_runner.invoke(app, ["--date", date.strftime("%Y-%m-%d"), "Username", "PortfolioName", "breakdown"])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(portfolio_id, date, output_dir=None)
    mock_plot_breakdown.execute.assert_called_once()


def test_growth_command(
    app: Typer, cli_runner: CliRunner, portfolio_id: int, mock_resolve_portfolio: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.resolve_portfolio", mock_resolve_portfolio)

    mock_plot_growth = mocker.MagicMock()
    mock_plot_growth.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=PlotGrowth, return_value=mock_plot_growth)
    mocker.patch("pyp.cli.plot.PlotGrowth", mock_class)

    date = datetime(2024, 12, 9)

    result = cli_runner.invoke(app, ["--date", date.strftime("%Y-%m-%d"), "Username", "PortfolioName", "growth"])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(portfolio_id, date, output_dir=None)
    mock_plot_growth.execute.assert_called_once()


def test_growth_breakdown_command(
    app: Typer, cli_runner: CliRunner, portfolio_id: int, mock_resolve_portfolio: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.resolve_portfolio", mock_resolve_portfolio)

    mock_plot_growth_breakdown = mocker.MagicMock()
    mock_plot_growth_breakdown.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=PlotGrowthBreakdown, return_value=mock_plot_growth_breakdown)
    mocker.patch("pyp.cli.plot.PlotGrowthBreakdown", mock_class)

    mock_class_mom = mocker.MagicMock(spec=PlotGrowthBreakdownMonthOverMonth)
    mocker.patch("pyp.cli.plot.PlotGrowthBreakdownMonthOverMonth", mock_class)

    date = datetime(2024, 12, 9)

    result = cli_runner.invoke(
        app, ["--date", date.strftime("%Y-%m-%d"), "Username", "PortfolioName", "growth-breakdown"]
    )

    assert result.exit_code == 0

    mock_class.assert_called_once_with(portfolio_id, date, output_dir=None)
    mock_plot_growth_breakdown.execute.assert_called_once()

    mock_class_mom.assert_not_called()


def test_growth_breakdown_command_with_month_over_month(
    app: Typer, cli_runner: CliRunner, portfolio_id: int, mock_resolve_portfolio: MagicMock, mocker: MockFixture
) -> None:
    mocker.patch("pyp.cli.plot.resolve_portfolio", mock_resolve_portfolio)

    mock_plot_growth_breakdown_mom = mocker.MagicMock()
    mock_plot_growth_breakdown_mom.execute = mocker.MagicMock()
    mock_class_mom = mocker.MagicMock(
        spec=PlotGrowthBreakdownMonthOverMonth, return_value=mock_plot_growth_breakdown_mom
    )
    mocker.patch("pyp.cli.plot.PlotGrowthBreakdownMonthOverMonth", mock_class_mom)

    mock_class = mocker.MagicMock(spec=PlotGrowthBreakdown)
    mocker.patch("pyp.cli.plot.PlotGrowthBreakdown", mock_class)

    date = datetime(2024, 12, 9)

    result = cli_runner.invoke(
        app,
        ["--date", date.strftime("%Y-%m-%d"), "Username", "PortfolioName", "growth-breakdown", "--month-over-month"],
    )

    assert result.exit_code == 0

    mock_class_mom.assert_called_once_with(portfolio_id, date, output_dir=None)
    mock_plot_growth_breakdown_mom.execute.assert_called_once()

    mock_class.assert_not_called()
