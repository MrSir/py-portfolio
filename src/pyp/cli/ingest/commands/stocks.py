from sqlalchemy import select
from sqlalchemy.orm import Session
from yfinance import Ticker

from pyp.database.engine import engine
from pyp.database.models import Stock


class IngestStocks:
    @property
    def stocks(self) -> list[Stock]:
        with Session(engine) as session:
            stocks = session.scalars(select(Stock)).all()

        return stocks

    @property
    def monikers(self) -> list[str]:
        return [stock.moniker for stock in self.stocks]

    def update_stock_info(self) -> None:
        with Session(engine) as session:
            for stock in self.stocks:
                session.add(stock)

                ticker = Ticker(stock.moniker)
                info = ticker.info

                stock.name = info["longName"]
                stock.description = info["longBusinessSummary"]

                session.commit()

    def update_stock_holdings(self) -> None:
        pass

    def update_stock_pricing(self) -> None:
        pass

    def ingest(self) -> None:
        self.update_stock_info()

        # for moniker in self.monikers:
        #     ticker = Ticker(moniker)
        #     # Fund data and holdings
        #     # ticker.fund_data.sector_weightings
        #
        #     with open(f"{moniker}.json", "w") as file:
        #           file.write(json.dumps(ticker.funds_data.sector_weightings))

        # Download pricing data
        # data: DataFrame = download(self.monikers, period="1y")
        #
        # with open("download.csv", "w") as file:
        #     file.write(data.to_csv())
