from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.portfolio import portfolio_app
from pyp.cli.portfolio.commands import AddMonikerCommand, AddSharesCommand
from pyp.database.engine import engine
from pyp.database.models import Portfolio


@pytest.fixture
def app() -> Typer:
    return portfolio_app


def test_main_app(app: Typer) -> None:
    assert "portfolio" == app.info.name
    assert "Manage portfolio DB entities." == app.info.help


def test_commands(app: Typer) -> None:
    assert ["add", "add-shares"] == [c.name for c in app.registered_commands]


def test_add_command(
    app: Typer, cli_runner: CliRunner, portfolio: Portfolio, mock_resolve_portfolio: MagicMock, mocker: MockerFixture
) -> None:
    mock_add = mocker.MagicMock()
    mock_add.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=AddMonikerCommand, return_value=mock_add)
    mocker.patch("pyp.cli.portfolio.AddMonikerCommand", mock_class)

    username = "MrSir"
    moniker = "QQQ"

    mocker.patch("pyp.cli.portfolio.resolve_portfolio", mock_resolve_portfolio)

    result = cli_runner.invoke(app, [username, portfolio.name, "add", moniker])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, portfolio, moniker)
    mock_add.execute.assert_called_once()


def test_add_shares_command(
    app: Typer, cli_runner: CliRunner, portfolio: Portfolio, mock_resolve_portfolio: MagicMock, mocker: MockerFixture
) -> None:
    mock_add = mocker.MagicMock()
    mock_add.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=AddSharesCommand, return_value=mock_add)
    mocker.patch("pyp.cli.portfolio.AddSharesCommand", mock_class)

    username = "MrSir"
    moniker = "QQQ"
    amount = 1.2
    price = 123.45
    purchased_on = datetime(2024, 12, 18)

    mocker.patch("pyp.cli.portfolio.resolve_portfolio", mock_resolve_portfolio)

    result = cli_runner.invoke(
        app,
        [
            username,
            portfolio.name,
            "add-shares",
            moniker,
            str(amount),
            str(price),
            purchased_on.strftime("%Y-%m-%d"),
        ],
    )

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, portfolio, moniker, amount, price, purchased_on)
    mock_add.execute.assert_called_once()
