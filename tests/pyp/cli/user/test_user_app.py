import pytest
from pytest_mock import MockerFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.user import user_app
from pyp.cli.user.commands import AddPortfolioCommand, AddUserCommand
from pyp.database.engine import engine


@pytest.fixture
def app() -> Typer:
    return user_app


def test_main_app(app: Typer) -> None:
    assert "user" == app.info.name
    assert "Manage user DB entities." == app.info.help


def test_commands(app: Typer) -> None:
    assert ["add", "add-portfolio"] == [c.name for c in app.registered_commands]


def test_add_command(app: Typer, cli_runner: CliRunner, mocker: MockerFixture) -> None:
    mock_add = mocker.MagicMock()
    mock_add.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=AddUserCommand, return_value=mock_add)
    mocker.patch("pyp.cli.user.AddUserCommand", mock_class)

    username = "MrSir"
    result = cli_runner.invoke(app, ["add", username])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, username)
    mock_add.execute.assert_called_once()


def test_add_portfolio_command(app: Typer, cli_runner: CliRunner, mocker: MockerFixture) -> None:
    mock_add = mocker.MagicMock()
    mock_add.execute = mocker.MagicMock()
    mock_class = mocker.MagicMock(spec=AddPortfolioCommand, return_value=mock_add)
    mocker.patch("pyp.cli.user.AddPortfolioCommand", mock_class)

    username = "MrSir"
    name = "My Portfolio"
    result = cli_runner.invoke(app, ["add-portfolio", username, name])

    assert result.exit_code == 0

    mock_class.assert_called_once_with(engine, username, name)
    mock_add.execute.assert_called_once()
