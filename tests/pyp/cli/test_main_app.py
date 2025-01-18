from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.commands.output.breakdown import OutputBreakdownCommand
from pyp.cli.commands.output.growth import OutputGrowthCommand
from pyp.cli.commands.output.growth_breakdown import (
    OutputGrowthBreakdownCommand,
    OutputGrowthBreakdownMonthOverMonthCommand,
)
from pyp.cli.commands.output.summary import OutputSummaryCommand
from pyp.cli.commands.setup import SetupCommand
from pyp.cli.currencies import currency_app
from pyp.cli.ingest import ingest_app
from pyp.cli.main import app as main_app
from pyp.cli.portfolio import portfolio_app
from pyp.cli.user import user_app
from pyp.database.engine import engine


@pytest.fixture
def app() -> Typer:
    return main_app


def test_main_app(app: Typer) -> None:
    assert "pyp" == app.info.name
    assert "PyPortfolio is a python tool for visualizing your financial portfolio." == app.info.help


def test_groups(app: Typer) -> None:
    assert [
        currency_app.info.name,
        ingest_app.info.name,
        user_app.info.name,
        portfolio_app.info.name,
    ] == [g.name for g in app.registered_groups]


@pytest.mark.parametrize(
    "name,help_txt",
    [
        ("setup", "Creates the database and sets up the project."),
        ("output", "Output various chart data of the portfolio."),
    ],
)
def test_commands(name: str, help_txt: str, app: Typer) -> None:
    assert (name, help_txt) in [(c.name, c.help) for c in app.registered_commands]


def test_setup_command(
    app: Typer,
    cli_runner: CliRunner,
    portfolio_id: int,
    mock_resolve_portfolio: MagicMock,
    mocker: MockerFixture,
) -> None:
    mock_setup = mocker.MagicMock()
    mock_setup.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=SetupCommand, return_value=mock_setup)
    mocker.patch("pyp.cli.main.SetupCommand", mock_class)

    result = cli_runner.invoke(app, ["setup", "--seed"])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, True)
    mock_setup.execute.assert_called_once()


def test_output_command(
    app: Typer,
    cli_runner: CliRunner,
    portfolio_id: int,
    mock_resolve_portfolio: MagicMock,
    mock_engine: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.main.engine", mock_engine)
    mocker.patch("pyp.cli.main.resolve_portfolio", mock_resolve_portfolio)

    mock_output_summary = mocker.MagicMock()
    mock_output_summary.execute = mocker.MagicMock()
    mock_summary_class = mocker.MagicMock(spec=OutputSummaryCommand, return_value=mock_output_summary)
    mocker.patch("pyp.cli.main.OutputSummaryCommand", mock_summary_class)

    mock_output_growth = mocker.MagicMock()
    mock_output_growth.execute = mocker.MagicMock()
    mock_growth_class = mocker.MagicMock(spec=OutputGrowthCommand, return_value=mock_output_growth)
    mocker.patch("pyp.cli.main.OutputGrowthCommand", mock_growth_class)

    mock_output_breakdown = mocker.MagicMock()
    mock_output_breakdown.execute = mocker.MagicMock()
    mock_breakdown_class = mocker.MagicMock(spec=OutputBreakdownCommand, return_value=mock_output_breakdown)
    mocker.patch("pyp.cli.main.OutputBreakdownCommand", mock_breakdown_class)

    mock_output_growth_breakdown = mocker.MagicMock()
    mock_output_growth_breakdown.execute = mocker.MagicMock()
    mock_growth_breakdown_class = mocker.MagicMock(
        spec=OutputGrowthBreakdownCommand, return_value=mock_output_growth_breakdown
    )
    mocker.patch("pyp.cli.main.OutputGrowthBreakdownCommand", mock_growth_breakdown_class)

    mock_output_growth_breakdown_mom = mocker.MagicMock()
    mock_output_growth_breakdown_mom.execute = mocker.MagicMock()
    mock_growth_breakdown_mom_class = mocker.MagicMock(
        spec=OutputGrowthBreakdownMonthOverMonthCommand, return_value=mock_output_growth_breakdown_mom
    )
    mocker.patch("pyp.cli.main.OutputGrowthBreakdownMonthOverMonthCommand", mock_growth_breakdown_mom_class)

    username = "MrSir"
    portfolio_name = "My Portfolio"
    date = datetime(2024, 12, 9)
    currency_code = "CAD"

    result = cli_runner.invoke(
        app,
        [
            "output",
            "--date",
            date.strftime("%Y-%m-%d"),
            "--currency",
            currency_code,
            username,
            portfolio_name,
        ],
    )

    assert result.exit_code == 0

    mock_summary_class.assert_called_once_with(mock_engine, username, portfolio_name, portfolio_id, date, currency_code)
    mock_growth_class.assert_called_once_with(mock_engine, portfolio_id, date, currency_code)
    mock_breakdown_class.assert_called_once_with(mock_engine, portfolio_id, date, currency_code)
    mock_growth_breakdown_class.assert_called_once_with(mock_engine, portfolio_id, date, currency_code)
    mock_growth_breakdown_mom_class.assert_called_once_with(mock_engine, portfolio_id, date, currency_code)

    mock_output_summary.execute.assert_called_once()
    mock_output_growth.execute.assert_called_once()
    mock_output_breakdown.execute.assert_called_once()
    mock_output_growth_breakdown.execute.assert_called_once()
    mock_output_growth_breakdown_mom.execute.assert_called_once()
