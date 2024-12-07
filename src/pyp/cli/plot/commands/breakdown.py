from functools import cached_property

from pandas import DataFrame
from sqlalchemy import Selectable, select

from pyp.database.models import Currency, Portfolio, PortfolioStocks, Share, Stock


class PlotBreakdown:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio

    @property
    def db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Stock.stock_type,
                Stock.sector_weightings,
                Share.amount,
                Share.price,
                Share.purchased_on,
                Currency.name.label("currency"),
            )
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .join(Stock.currency)
            .where(PortfolioStocks.portfolio_id == self.portfolio.id)
        )

    @cached_property
    def db_data_df(self) -> DataFrame:
        return DataFrame()

        # with Session(engine) as session:
        #     assert session.bind
        #
        #     db_data_df = pd.read_sql(self.db_query, session.bind)
        #
        # return db_data_df

    def plot(self) -> None:
        pass
