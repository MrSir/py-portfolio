from datetime import datetime

import pytest
from pytest_mock import MockerFixture
from typer import Typer
from typer.testing import CliRunner

from pyp.cli.ingest import IngestExchangeRatesCommand, ingest_app
from pyp.cli.ingest.commands.stocks import IngestStocksCommand
from pyp.database.engine import engine


@pytest.fixture
def app() -> Typer:
    return ingest_app


def test_main_app(app: Typer) -> None:
    assert "ingest" == app.info.name
    assert "Ingest various market data from external services." == app.info.help


@pytest.mark.parametrize(
    "name,help_txt",
    [
        ("exchange-rates", "Ingest exchange rates from currencyapi.com."),
        ("stocks", "Ingest various stocks from Yahoo Finance."),
    ],
)
def test_commands(name: str, help_txt: str, app: Typer) -> None:
    assert (name, help_txt) in [(c.name, c.help) for c in app.registered_commands]


def test_exchange_rates_command(app: Typer, cli_runner: CliRunner, mocker: MockerFixture) -> None:
    mock_ingest_exchange_rates_command = mocker.MagicMock()
    mock_ingest_exchange_rates_command.execute = mocker.MagicMock()
    mock_ingest_exchange_rates_command_class = mocker.MagicMock(
        spec=IngestExchangeRatesCommand, return_value=mock_ingest_exchange_rates_command
    )
    mocker.patch("pyp.cli.ingest.IngestExchangeRatesCommand", mock_ingest_exchange_rates_command_class)

    api_key = "API-KEY"

    mock_dotenv = mocker.MagicMock(return_value={"FREE_CURRENCY_API_KEY": api_key})
    mocker.patch("pyp.cli.ingest.dotenv_values", mock_dotenv)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    result = cli_runner.invoke(app, ["exchange-rates", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])

    assert result.exit_code == 0

    mock_ingest_exchange_rates_command_class.assert_called_once_with(engine, start_date, end_date, api_key)
    mock_ingest_exchange_rates_command.execute.assert_called_once()


def test_exchange_rates_command_env_key_does_not_exist(
    app: Typer, cli_runner: CliRunner, mocker: MockerFixture
) -> None:
    mock_ingest_exchange_rates_command = mocker.MagicMock()
    mock_ingest_exchange_rates_command.execute = mocker.MagicMock()
    mock_ingest_exchange_rates_command_class = mocker.MagicMock(
        spec=IngestExchangeRatesCommand, return_value=mock_ingest_exchange_rates_command
    )
    mocker.patch("pyp.cli.ingest.IngestExchangeRatesCommand", mock_ingest_exchange_rates_command_class)

    mock_dotenv = mocker.MagicMock(return_value={})
    mocker.patch("pyp.cli.ingest.dotenv_values", mock_dotenv)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    result = cli_runner.invoke(app, ["exchange-rates", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])

    assert isinstance(result.exception, ValueError)
    assert result.exception.args[0] == "FREE_CURRENCY_API_KEY environment variable is not set."

    mock_ingest_exchange_rates_command_class.assert_not_called()
    mock_ingest_exchange_rates_command.execute.assert_not_called()


def test_exchange_rates_command_env_key_is_none(app: Typer, cli_runner: CliRunner, mocker: MockerFixture) -> None:
    mock_ingest_exchange_rates_command = mocker.MagicMock()
    mock_ingest_exchange_rates_command.execute = mocker.MagicMock()
    mock_ingest_exchange_rates_command_class = mocker.MagicMock(
        spec=IngestExchangeRatesCommand, return_value=mock_ingest_exchange_rates_command
    )
    mocker.patch("pyp.cli.ingest.IngestExchangeRatesCommand", mock_ingest_exchange_rates_command_class)

    mock_dotenv = mocker.MagicMock(return_value={"FREE_CURRENCY_API_KEY": None})
    mocker.patch("pyp.cli.ingest.dotenv_values", mock_dotenv)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    result = cli_runner.invoke(app, ["exchange-rates", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])

    assert isinstance(result.exception, ValueError)
    assert result.exception.args[0] == "FREE_CURRENCY_API_KEY environment variable is not set."

    mock_ingest_exchange_rates_command_class.assert_not_called()
    mock_ingest_exchange_rates_command.execute.assert_not_called()


def test_stocks_command(app: Typer, cli_runner: CliRunner, mocker: MockerFixture) -> None:
    mock_ingest_stocks_command = mocker.MagicMock()
    mock_ingest_stocks_command.execute = mocker.MagicMock()
    mock_ingest_stocks_command_class = mocker.MagicMock(
        spec=IngestStocksCommand, return_value=mock_ingest_stocks_command
    )
    mocker.patch("pyp.cli.ingest.IngestStocksCommand", mock_ingest_stocks_command_class)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    moniker_1 = "ADP"
    moniker_2 = "IYK"
    moniker_3 = "BTCO"
    moniker_4 = "SMH"
    result = cli_runner.invoke(
        app,
        [
            "stocks",
            "-sd",
            start_date.strftime("%Y-%m-%d"),
            "-ed",
            end_date.strftime("%Y-%m-%d"),
            "-m",
            moniker_1,
            "-m",
            moniker_2,
            "-em",
            moniker_3,
            "-em",
            moniker_4,
        ],
    )

    assert result.exit_code == 0

    mock_ingest_stocks_command_class.assert_called_once_with(
        engine,
        start_date=start_date,
        end_date=end_date,
        monikers=[moniker_1, moniker_2],
        exclude_monikers=[moniker_3, moniker_4],
    )
    mock_ingest_stocks_command.execute.assert_called_once()
