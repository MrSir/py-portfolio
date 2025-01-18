from datetime import datetime, timedelta
from unittest.mock import MagicMock, call

import freecurrencyapi
import pytest
from pytest_mock import MockerFixture

from pyp.cli.ingest.commands.base import IngestBaseCommand
from pyp.cli.ingest.commands.exchange_rates import IngestExchangeRatesCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Currency, ExchangeRate


@pytest.fixture
def command(mock_engine: MagicMock) -> IngestExchangeRatesCommand:
    return IngestExchangeRatesCommand(
        mock_engine,
        datetime(2024, 1, 1),
        datetime(2024, 12, 31),
        "API-KEY",
    )


@pytest.fixture
def mock_client(mocker: MockerFixture) -> MagicMock:
    m_client = mocker.MagicMock(spec=freecurrencyapi.Client)

    return m_client


def test_initialization(mock_engine: MagicMock) -> None:
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    api_key = "API-KEY"

    command = IngestExchangeRatesCommand(mock_engine, start_date, end_date, api_key)

    assert isinstance(command, IngestBaseCommand)
    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert start_date == command.start_date
    assert end_date == command.end_date
    assert api_key == command.api_key

    assert dict() == command._currency_pairs
    assert [] == command._exchange_rates_values


def test_client_returns_preset(command: IngestExchangeRatesCommand, mock_client: MagicMock) -> None:
    command._client = mock_client

    assert mock_client == command.client


def test_client_computes_when_none(
    command: IngestExchangeRatesCommand,
    mock_client: MagicMock,
    mocker: MockerFixture,
) -> None:
    mock_class = mocker.MagicMock(return_value=mock_client)
    mocker.patch("pyp.cli.ingest.commands.exchange_rates.freecurrencyapi.Client", mock_class)

    command._client = None

    assert mock_client == command.client

    mock_class.assert_called_once_with(command.api_key)


def test_compute_currency_pairs(command: IngestExchangeRatesCommand) -> None:
    command._currencies_by_code = {
        "USD": Currency(code="USD"),
        "CAD": Currency(code="CAD"),
        "EUR": Currency(code="EUR"),
    }

    command._compute_currency_pairs()

    expected_pairs = {"CAD": ["USD", "EUR"], "EUR": ["USD", "CAD"], "USD": ["CAD", "EUR"]}

    assert expected_pairs == command._currency_pairs


def test_download_exchange_rates_for(
    command: IngestExchangeRatesCommand,
    mock_client: MagicMock,
    mocker: MockerFixture,
) -> None:
    usd = Currency(id=1, code="USD")
    cad = Currency(id=2, code="CAD")
    eur = Currency(id=3, code="EUR")

    date = datetime(2022, 1, 1)
    date_str = date.strftime("%Y-%m-%d")
    data = {"data": {date_str: {eur.code: 0.6965837353, usd.code: 0.7920854819}}}
    mock_client.historical = mocker.MagicMock(return_value=data)
    mock_class = mocker.MagicMock(return_value=mock_client)
    mocker.patch("pyp.cli.ingest.commands.exchange_rates.freecurrencyapi.Client", mock_class)

    command._currencies_by_code = {usd.code: usd, cad.code: cad, eur.code: eur}

    expected_exchange_rates_values = [
        {"from_currency_id": cad.id, "to_currency_id": usd.id, "date": date, "rate": data["data"][date_str][usd.code]},
        {"from_currency_id": cad.id, "to_currency_id": eur.id, "date": date, "rate": data["data"][date_str][eur.code]},
    ]

    exchange_rate_values = command._download_exchange_rates_for(date, cad.code, [usd.code, eur.code])

    assert expected_exchange_rates_values == exchange_rate_values

    mock_client.historical.assert_called_once_with(
        date_str,
        base_currency=cad.code,
        currencies=[usd.code, eur.code],
    )


def test_download_exchange_rates(command: IngestExchangeRatesCommand, mocker: MockerFixture) -> None:
    usd = Currency(id=1, code="USD")
    cad = Currency(id=2, code="CAD")
    eur = Currency(id=3, code="EUR")
    command._currency_pairs = {
        usd.code: [cad.code, eur.code],
        cad.code: [usd.code, eur.code],
        eur.code: [usd.code, cad.code],
    }
    rates_for_usd = ["rates_for_usd"]
    rates_for_cad = ["rates_for_cad"]
    rates_for_eur = ["rates_for_eur"]

    mock_derf = mocker.MagicMock(
        side_effect=[rates_for_usd, rates_for_cad, rates_for_eur, rates_for_usd, rates_for_cad, rates_for_eur]
    )
    mocker.patch.object(command, "_download_exchange_rates_for", mock_derf)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)

    command.start_date = start_date
    command.end_date = end_date

    command._download_exchange_rates()

    expected_values = rates_for_usd + rates_for_cad + rates_for_eur + rates_for_usd + rates_for_cad + rates_for_eur

    assert expected_values == command._exchange_rates_values

    mock_derf.assert_has_calls([
        call(start_date, usd.code, [cad.code, eur.code]),
    ])


def test_prepare_exchange_rates_upsert_statement(command: IngestExchangeRatesCommand, mocker: MockerFixture) -> None:
    exchange_rate_values = [dict()]

    mock_statement = mocker.MagicMock()
    mock_statement.values = mocker.MagicMock(return_value=mock_statement)
    mock_statement.on_conflict_do_update = mocker.MagicMock(return_value=mock_statement)
    mock_insert = mocker.MagicMock(return_value=mock_statement)
    mocker.patch("pyp.cli.ingest.commands.exchange_rates.insert", mock_insert)

    command._exchange_rates_values = exchange_rate_values

    actual_statement = command._prepare_exchange_rates_upsert_statement()

    assert mock_statement == actual_statement
    mock_insert.assert_called_once_with(ExchangeRate)
    mock_statement.values.assert_called_once_with(exchange_rate_values)
    mock_statement.on_conflict_do_update.assert_called_once_with(
        index_elements=["from_currency_id", "to_currency_id", "date"],
        set_={"rate": mock_statement.excluded.rate},
    )


def test_update_exchange_rates(
    command: IngestExchangeRatesCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.ingest.commands.exchange_rates.Session", mock_session_class)

    mock_statement = mocker.MagicMock()
    mock_perus = mocker.MagicMock(return_value=mock_statement)
    mocker.patch.object(command, "_prepare_exchange_rates_upsert_statement", mock_perus)

    moniker_1 = "ADP"
    moniker_2 = "IYK"
    command._monikers = [moniker_1, moniker_2]

    command._update_exchange_rates()

    mock_session_class.assert_called_once_with(command.engine)
    mock_perus.assert_called_once()
    mock_session.execute.assert_called_once_with(mock_statement)
    mock_session.commit.assert_called_once()


def test_execute(command: IngestExchangeRatesCommand, mocker: MockerFixture) -> None:
    mock_rc = mocker.MagicMock()
    mock_ccp = mocker.MagicMock()
    mock_der = mocker.MagicMock()
    mock_uer = mocker.MagicMock()
    mocker.patch.object(command, "_resolve_currencies", mock_rc)
    mocker.patch.object(command, "_compute_currency_pairs", mock_ccp)
    mocker.patch.object(command, "_download_exchange_rates", mock_der)
    mocker.patch.object(command, "_update_exchange_rates", mock_uer)

    command.execute()

    mock_rc.assert_called_once()
    mock_ccp.assert_called_once()
    mock_der.assert_called_once()
    mock_uer.assert_called_once()
