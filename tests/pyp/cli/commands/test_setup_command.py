from datetime import datetime
from unittest.mock import MagicMock, call

import pytest
from pytest_mock import MockerFixture

from pyp.cli.commands.setup import SetupCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Currency, ExchangeRate


@pytest.fixture
def command(mock_engine: MagicMock) -> SetupCommand:
    return SetupCommand(mock_engine)


@pytest.fixture
def default_currencies() -> list[Currency]:
    currencies = [
        {"code": "USD", "name": "United States Dollar"},
        {"code": "CAD", "name": "Canadian Dollar"},
    ]

    return [Currency(**c) for c in currencies]


def test_initialization(mock_engine: MagicMock) -> None:
    seed = True

    command = SetupCommand(mock_engine, seed=seed)

    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert seed == command.seed


def test_create_schema(command: SetupCommand, mocker: MockerFixture) -> None:
    mock_create_all = mocker.MagicMock()
    mocker.patch("pyp.cli.commands.setup.Base.metadata.create_all", mock_create_all)

    command._create_schema()

    mock_create_all.assert_called_once_with(command.engine)


def test_read_currencies(command: SetupCommand) -> None:
    currencies = [
        {"code": "USD", "name": "United States Dollar"},
        {"code": "CAD", "name": "Canadian Dollar"},
        {"code": "EUR", "name": "Euro"},
    ]

    assert currencies == command._read_currencies()


def test_seed_currencies(
    command: SetupCommand,
    mock_engine: MagicMock,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.commands.setup.Session", mock_session_class)

    mock_query = mocker.MagicMock()
    mock_query.values = mocker.MagicMock(return_value=mock_query)
    mock_query.on_conflict_do_nothing = mocker.MagicMock(return_value=mock_query)
    mock_insert = mocker.MagicMock(return_value=mock_query)
    mocker.patch("pyp.cli.commands.setup.insert", mock_insert)

    command._seed_currencies()

    mock_session_class.assert_called_once_with(mock_engine)
    mock_insert.assert_called_once_with(Currency)
    mock_query.values.assert_called_once_with(command._read_currencies())
    mock_query.on_conflict_do_nothing.assert_called_once_with(index_elements=["code"])
    mock_session.execute.assert_called_once_with(mock_query)
    mock_session.commit.assert_called_once()


def test_resolve_currencies(
    command: SetupCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    currencies = [Currency(id=1, code="USD"), Currency(id=2, code="CAD")]

    mocker.patch("pyp.cli.commands.setup.Session", mock_session_class)
    mocker.patch("pyp.cli.commands.setup.select", mock_select)
    mock_session.scalars.return_value.all.return_value = currencies

    command._resolve_currency_ids()

    assert {c.code: c.id for c in currencies} == command._currency_id_by_code

    mock_session_class.assert_called_once_with(command.engine)
    mock_session.scalars.assert_called_once()
    mock_select.assert_called_once_with(Currency)


def test_read_exchange_rates_for_code(command: SetupCommand, mocker: MockerFixture) -> None:
    usd = Currency(id=1, code="USD")
    cad = Currency(id=2, code="CAD")
    eur = Currency(id=3, code="EUR")
    currencies = [usd, cad, eur]
    command._currency_id_by_code = {c.code: c.id for c in currencies}

    date_1 = datetime(2022, 1, 1)
    file_name_1 = f"{date_1}.json"
    usd_to_cad_1 = 1.26249
    usd_to_eur_1 = 0.87943
    rates_json_1 = (
        '{"data": {"'
        + date_1.strftime("%Y-%m-%d")
        + '": '
        + '{"'
        + cad.code
        + '": '
        + str(usd_to_cad_1)
        + ', "'
        + eur.code
        + '": '
        + str(usd_to_eur_1)
        + "}}}"
    )

    date_2 = datetime(2022, 1, 2)
    file_name_2 = f"{date_2}.json"
    usd_to_cad_2 = 1.2625
    usd_to_eur_2 = 0.87929
    rates_json_2 = (
        '{"data": {"'
        + date_2.strftime("%Y-%m-%d")
        + '": '
        + '{"'
        + cad.code
        + '": '
        + str(usd_to_cad_2)
        + ', "'
        + eur.code
        + '": '
        + str(usd_to_eur_2)
        + "}}}"
    )

    mock_glob = mocker.MagicMock(return_value=[file_name_1, file_name_2])
    mocker.patch("pyp.cli.commands.setup.Path.glob", mock_glob)

    mock_file = mocker.MagicMock()
    mock_file.read = mocker.MagicMock(side_effect=[rates_json_1, rates_json_2])
    mock_open = mocker.MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mocker.patch("pyp.cli.commands.setup.open", mock_open)

    expected_exchange_rates = [
        {"date": date_1, "from_currency_id": usd.id, "to_currency_id": cad.id, "rate": usd_to_cad_1},
        {"date": date_1, "from_currency_id": usd.id, "to_currency_id": eur.id, "rate": usd_to_eur_1},
        {"date": date_2, "from_currency_id": usd.id, "to_currency_id": cad.id, "rate": usd_to_cad_2},
        {"date": date_2, "from_currency_id": usd.id, "to_currency_id": eur.id, "rate": usd_to_eur_2},
    ]

    assert expected_exchange_rates == command._read_exchange_rates_for_code(usd.code)


def test_seed_exchange_rates(
    command: SetupCommand,
    mock_engine: MagicMock,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.commands.setup.Session", mock_session_class)

    mock_query = mocker.MagicMock()
    mock_query.values = mocker.MagicMock(return_value=mock_query)
    mock_query.on_conflict_do_nothing = mocker.MagicMock(return_value=mock_query)
    mock_insert = mocker.MagicMock(return_value=mock_query)
    mocker.patch("pyp.cli.commands.setup.insert", mock_insert)

    exchange_rates: dict = dict()
    mock_rerfc = mocker.MagicMock(return_value=exchange_rates)
    mocker.patch.object(command, "_read_exchange_rates_for_code", mock_rerfc)

    usd = Currency(id=1, code="USD")
    cad = Currency(id=2, code="CAD")
    eur = Currency(id=3, code="EUR")
    currencies = [usd, cad, eur]
    command._currency_id_by_code = {c.code: c.id for c in currencies}

    command._seed_exchange_rates()

    mock_session_class.assert_called_once_with(mock_engine)
    mock_insert.assert_has_calls([
        call(ExchangeRate),
        call(ExchangeRate),
        call(ExchangeRate),
    ])
    mock_query.values.assert_has_calls([
        call(exchange_rates),
        call(exchange_rates),
        call(exchange_rates),
    ])
    mock_query.on_conflict_do_nothing.assert_has_calls([
        call(index_elements=["from_currency_id", "to_currency_id", "date"]),
        call(index_elements=["from_currency_id", "to_currency_id", "date"]),
        call(index_elements=["from_currency_id", "to_currency_id", "date"]),
    ])
    mock_session.execute.assert_has_calls([
        call(mock_query),
        call(mock_query),
        call(mock_query),
    ])
    mock_session.commit.assert_has_calls([call(), call(), call()])


def test_execute_without_seed(command: SetupCommand, mocker: MockerFixture) -> None:
    mock_create_schema = mocker.MagicMock()
    mocker.patch.object(command, "_create_schema", mock_create_schema)
    mock_seed_currencies = mocker.MagicMock()
    mocker.patch.object(command, "_seed_currencies", mock_seed_currencies)

    command.seed = False
    command.execute()

    mock_create_schema.assert_called_once()
    mock_seed_currencies.assert_not_called()


def test_execute_with_seed(command: SetupCommand, mocker: MockerFixture) -> None:
    mock_create_schema = mocker.MagicMock()
    mocker.patch.object(command, "_create_schema", mock_create_schema)
    mock_seed_currencies = mocker.MagicMock()
    mocker.patch.object(command, "_seed_currencies", mock_seed_currencies)
    mock_resolve_currency_ids = mocker.MagicMock()
    mocker.patch.object(command, "_resolve_currency_ids", mock_resolve_currency_ids)
    mock_seed_exchange_rates = mocker.MagicMock()
    mocker.patch.object(command, "_seed_exchange_rates", mock_seed_exchange_rates)

    command.seed = True
    command.execute()

    mock_create_schema.assert_called_once()
    mock_seed_currencies.assert_called_once()
    mock_resolve_currency_ids.assert_called_once()
    mock_seed_exchange_rates.assert_called_once()
