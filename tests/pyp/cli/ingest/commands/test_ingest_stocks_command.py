import json
from datetime import datetime
from unittest.mock import MagicMock, call

import pytest
from pandas import DataFrame, Index, Series
from pytest_mock import MockerFixture
from yfinance import Ticker
from yfinance.scrapers.funds import FundsData

from pyp.cli.ingest.commands.base import IngestBaseCommand
from pyp.cli.ingest.commands.stocks import IngestStocksCommand
from pyp.cli.protocols import CommandProtocol
from pyp.database.models import Currency, Price, Stock

from .test_data.ADP import adp_info
from .test_data.BTCO import btco_info
from .test_data.IYK import iyk_info, iyk_sector_weightings


@pytest.fixture
def command(mock_engine: MagicMock) -> IngestStocksCommand:
    return IngestStocksCommand(
        mock_engine,
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 12, 31),
    )


def test_initialization(mock_engine: MagicMock) -> None:
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    monikers = ["ADP", "IYK"]
    exclude_monikers = ["BTCO"]

    command = IngestStocksCommand(
        mock_engine,
        start_date=start_date,
        end_date=end_date,
        monikers=monikers,
        exclude_monikers=exclude_monikers,
    )

    assert isinstance(command, IngestBaseCommand)
    assert isinstance(command, CommandProtocol)

    assert mock_engine == command.engine
    assert start_date == command.start_date
    assert end_date == command.end_date
    assert monikers == command.monikers
    assert exclude_monikers == command.exclude_monikers


@pytest.mark.parametrize(
    "query,monikers,exclude_monikers",
    [
        (
            """SELECT
                stocks.id,
                stocks.currency_id,
                stocks.stock_type,
                stocks.moniker,
                stocks.name,
                stocks.description,
                stocks.sector_weightings
            FROM stocks""",
            None,
            None,
        ),
        (
            """SELECT
                stocks.id,
                stocks.currency_id,
                stocks.stock_type,
                stocks.moniker,
                stocks.name,
                stocks.description,
                stocks.sector_weightings
            FROM stocks
            WHERE stocks.moniker IN (__[POSTCOMPILE_moniker_1])""",
            ["ADP", "IYK"],
            None,
        ),
        (
            """SELECT
                stocks.id,
                stocks.currency_id,
                stocks.stock_type,
                stocks.moniker, stocks.name,
                stocks.description,
                stocks.sector_weightings
            FROM stocks
            WHERE (stocks.moniker NOT IN (__[POSTCOMPILE_moniker_1]))""",
            None,
            ["BTCO", "SMH"],
        ),
        (
            """SELECT
                stocks.id,
                stocks.currency_id,
                stocks.stock_type,
                stocks.moniker,
                stocks.name,
                stocks.description,
                stocks.sector_weightings
            FROM stocks
            WHERE stocks.moniker IN (__[POSTCOMPILE_moniker_1])
            AND (stocks.moniker NOT IN (__[POSTCOMPILE_moniker_2]))""",
            ["ADP", "IYK"],
            ["BTCO", "SMH"],
        ),
    ],
)
def test_prepare_stocks_statement(
    query: str,
    monikers: list[str] | None,
    exclude_monikers: list[str] | None,
    command: IngestStocksCommand,
) -> None:
    command.monikers = monikers
    command.exclude_monikers = exclude_monikers

    actual_statement = command._prepare_stocks_statement()

    select_query = query.replace("\n                ", " ").replace("\n            ", " \n").replace(" \nAND", " AND")

    assert select_query == str(actual_statement)


def test_resolve_stocks(
    command: IngestStocksCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mock_select: MagicMock,
    mocker: MockerFixture,
) -> None:
    stocks = [Stock(id=1, moniker="ADP"), Stock(id=2, moniker="SMH")]

    mocker.patch("pyp.cli.ingest.commands.stocks.Session", mock_session_class)
    mocker.patch("pyp.cli.ingest.commands.stocks.select", mock_select)
    mock_session.scalars.return_value.all.return_value = stocks

    command._resolve_stocks()

    assert stocks == command._stocks
    assert {s.moniker: s.id for s in stocks} == command._stock_ids_by_moniker
    assert [s.moniker for s in stocks] == command._monikers

    mock_session_class.assert_called_once_with(command.engine)
    mock_session.scalars.assert_called_once()
    mock_select.assert_called_once_with(Stock)


@pytest.mark.parametrize(
    "moniker,info,sector_weightings,expected_sector_weightings",
    [
        ("ADP", adp_info, None, {adp_info["sectorKey"]: 1.0}),
        ("BTCO", btco_info, None, {btco_info["category"].replace(" ", "_").lower(): 1.0}),  # type: ignore[attr-defined]
        ("IYK", iyk_info, iyk_sector_weightings, iyk_sector_weightings),
    ],
)
def test_prepare_updated_stock(
    moniker: str,
    info: dict,
    sector_weightings: dict | None,
    expected_sector_weightings: dict,
    command: IngestStocksCommand,
    mocker: MockerFixture,
) -> None:
    currencies = [Currency(code="USD")]
    command._currencies_by_code = {c.code: c for c in currencies}

    stock = Stock(moniker=moniker)

    assert stock.stock_type is None
    assert stock.name is None
    assert stock.description is None
    assert stock.currency is None
    assert stock.sector_weightings is None

    mock_ticker = mocker.MagicMock(spec=Ticker)
    mock_ticker.info = info
    mock_funds_data = mocker.MagicMock(spec=FundsData)
    mock_funds_data.sector_weightings = sector_weightings
    mock_ticker.funds_data = mock_funds_data
    mock_ticker_class = mocker.MagicMock(spec=Ticker, return_value=mock_ticker)
    mocker.patch("pyp.cli.ingest.commands.stocks.Ticker", mock_ticker_class)

    command._prepare_updated_stock(stock)

    assert info["quoteType"] == stock.stock_type
    assert info["longName"] == stock.name
    assert info["longBusinessSummary"] == stock.description
    assert info["longBusinessSummary"] == stock.description
    assert command._currencies_by_code[info["currency"]] == stock.currency
    assert json.dumps(expected_sector_weightings) == stock.sector_weightings


def test_update_stock_info(
    command: IngestStocksCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    stock_adp = Stock(moniker="ADP")
    stock_smh = Stock(moniker="SMH")
    stocks = [stock_adp, stock_smh]
    command._stocks = stocks

    currencies = [Currency(code="USD"), Currency(code="CAD")]
    command._currencies_by_code = {c.code: c for c in currencies}

    mocker.patch("pyp.cli.ingest.commands.stocks.Session", mock_session_class)

    mock_prepare_updated_stock = mocker.MagicMock()
    mocker.patch.object(command, "_prepare_updated_stock", mock_prepare_updated_stock)

    command._update_stock_info()

    mock_session_class.assert_called_once_with(command.engine)
    mock_prepare_updated_stock.assert_has_calls([
        call(stock_adp),
        call(stock_smh),
    ])
    mock_session.add.assert_has_calls([
        call(stock_adp),
        call(stock_smh),
    ])
    mock_session.commit.assert_has_calls([call(), call()])


def test_prices_download_parameters(command: IngestStocksCommand) -> None:
    command.start_date = None
    command.end_date = None

    assert {"period": "1y"} == command._prices_download_parameters()


def test_prices_download_parameters_when_dates_set(command: IngestStocksCommand) -> None:
    assert command.start_date is not None
    assert command.end_date is not None

    assert {
        "start": command.start_date.strftime("%Y-%m-%d"),
        "end": command.end_date.strftime("%Y-%m-%d"),
    } == command._prices_download_parameters()


def test_download_prices_df(command: IngestStocksCommand, mocker: MockerFixture) -> None:
    monikers = ["ADP", "BTCO", "IYK"]
    command._monikers = monikers

    expected_df = DataFrame()

    mock_download = mocker.MagicMock(return_value=expected_df)
    mocker.patch("pyp.cli.ingest.commands.stocks.download", mock_download)

    download_params = {"period": "1y"}
    mock_prices_download_parameters = mocker.MagicMock(return_value=download_params)
    mocker.patch.object(command, "_prices_download_parameters", mock_prices_download_parameters)

    command._download_prices_df()

    assert isinstance(command._prices_df, DataFrame)
    assert expected_df.equals(command._prices_df)

    mock_prices_download_parameters.assert_called_once()
    mock_download.assert_called_once_with(monikers, keepna=True, rounding=True, **download_params)


def test_extract_close_prices_sr(command: IngestStocksCommand) -> None:
    monikers = ["ADP", "BTCO", "IYK"]
    command._monikers = monikers

    prices_df = DataFrame(data={"Open": [0, None, 123], "Close": [1.5, None, 45.1]})
    expected_series = prices_df["Close"].fillna(0)

    command._prices_df = prices_df
    command._extract_close_prices()

    actual_series = command._close_prices_df

    assert isinstance(actual_series, Series)
    assert expected_series.equals(actual_series)


def test_prepare_price_upsert_statement(command: IngestStocksCommand, mocker: MockerFixture) -> None:
    amount_1 = 1.23
    amount_2 = 0
    amount_3 = 4.54

    date_1 = "2024-01-01"
    date_2 = "2024-01-02"
    date_3 = "2024-01-03"

    moniker = "ADP"

    close_prices_df = DataFrame(
        data={moniker: Series([amount_1, amount_2, amount_3], index=Index([date_1, date_2, date_3], name="Date"))}
    )
    command._close_prices_df = close_prices_df

    stock_id = 1
    stock_ids_by_moniker = {moniker: stock_id}
    command._stock_ids_by_moniker = stock_ids_by_moniker

    mock_statement = mocker.MagicMock()
    mock_statement.values = mocker.MagicMock(return_value=mock_statement)
    mock_statement.on_conflict_do_update = mocker.MagicMock(return_value=mock_statement)
    mock_insert = mocker.MagicMock(return_value=mock_statement)
    mocker.patch("pyp.cli.ingest.commands.stocks.insert", mock_insert)

    actual_statement = command._prepare_price_upsert_statement(moniker)

    assert mock_statement == actual_statement
    mock_insert.assert_called_once_with(Price)
    mock_statement.values.assert_called_once_with([
        {"stock_id": stock_id, "date": date_1, "amount": amount_1},
        {"stock_id": stock_id, "date": date_2, "amount": amount_2},
        {"stock_id": stock_id, "date": date_3, "amount": amount_3},
    ])
    mock_statement.on_conflict_do_update.assert_called_once_with(
        index_elements=["stock_id", "date"],
        set_={"amount": mock_statement.excluded.amount},
    )


def test_update_stock_pricing(
    command: IngestStocksCommand,
    mock_session_class: MagicMock,
    mock_session: MagicMock,
    mocker: MockerFixture,
) -> None:
    mocker.patch("pyp.cli.ingest.commands.stocks.Session", mock_session_class)

    mock_statement = mocker.MagicMock()
    mock_ppus = mocker.MagicMock(return_value=mock_statement)
    mocker.patch.object(command, "_prepare_price_upsert_statement", mock_ppus)

    moniker_1 = "ADP"
    moniker_2 = "IYK"
    command._monikers = [moniker_1, moniker_2]

    command._update_stock_pricing()

    mock_session_class.assert_called_once_with(command.engine)
    mock_ppus.assert_has_calls([call(moniker_1), call(moniker_2)])
    mock_session.execute.assert_has_calls([call(mock_statement), call(mock_statement)])
    mock_session.commit.assert_has_calls([call(), call()])


def test_execute(command: IngestStocksCommand, mocker: MockerFixture) -> None:
    mock_rs = mocker.MagicMock()
    mock_rc = mocker.MagicMock()
    mock_usi = mocker.MagicMock()
    mock_dpdf = mocker.MagicMock()
    mock_ecp = mocker.MagicMock()
    mock_usp = mocker.MagicMock()
    mocker.patch.object(command, "_resolve_stocks", mock_rs)
    mocker.patch.object(command, "_resolve_currencies", mock_rc)
    mocker.patch.object(command, "_update_stock_info", mock_usi)
    mocker.patch.object(command, "_download_prices_df", mock_dpdf)
    mocker.patch.object(command, "_extract_close_prices", mock_ecp)
    mocker.patch.object(command, "_update_stock_pricing", mock_usp)

    command.execute()

    mock_rs.assert_called_once()
    mock_rc.assert_called_once()
    mock_usi.assert_called_once()
    mock_dpdf.assert_called_once()
    mock_ecp.assert_called_once()
    mock_usp.assert_called_once()
