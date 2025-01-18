import pytest
from pytest_mock import MockerFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.currencies import currency_app
from pyp.cli.currencies.commands import AddCurrencyCommand
from pyp.database.engine import engine


@pytest.fixture
def app() -> Typer:
    return currency_app


def test_main_app(app: Typer) -> None:
    assert "currency" == app.info.name
    assert "Manage currency DB entities." == app.info.help


def test_commands(app: Typer) -> None:
    assert ["add"] == [c.name for c in app.registered_commands]


def test_add_command(app: Typer, cli_runner: CliRunner, mocker: MockerFixture) -> None:
    mock_add = mocker.MagicMock()
    mock_add.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=AddCurrencyCommand, return_value=mock_add)
    mocker.patch("pyp.cli.currencies.AddCurrencyCommand", mock_class)

    code = "USD"
    name = '"United States Dollar"'

    result = cli_runner.invoke(app, [code, name])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, code, name)
    mock_add.execute.assert_called_once()
