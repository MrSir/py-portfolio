from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.commands import SetupCommand
from pyp.cli.currencies import currency_app
from pyp.cli.ingest import ingest
from pyp.cli.main import app as main_app
from pyp.cli.plot import plot_app
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
        ingest.info.name,
        plot_app.info.name,
        currency_app.info.name,
        user_app.info.name,
        portfolio_app.info.name,
    ] == [g.name for g in app.registered_groups]


def test_commands(app: Typer) -> None:
    assert ["setup"] == [c.name for c in app.registered_commands]


def test_setup_command(
    app: Typer,
    cli_runner: CliRunner,
    portfolio_id: int,
    mock_resolve_portfolio: MagicMock,
    mocker: MockFixture,
) -> None:
    mock_setup = mocker.MagicMock()
    mock_setup.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=SetupCommand, return_value=mock_setup)
    mocker.patch("pyp.cli.main.SetupCommand", mock_class)

    result = cli_runner.invoke(app, ["setup", "--seed"])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, True)
    mock_setup.execute.assert_called_once()
