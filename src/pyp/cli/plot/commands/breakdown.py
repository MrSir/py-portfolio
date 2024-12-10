import json

from matplotlib import pyplot as plt
from pandas import DataFrame
from sqlalchemy import Selectable, func, select

from pyp.cli.plot.commands.base import PlotCommand
from pyp.database.models import PortfolioStocks, Price, Share, Stock


class PlotBreakdown(PlotCommand):
    @property
    def db_query(self) -> Selectable:
        return (
            select(
                Stock.moniker,
                Stock.stock_type,
                Stock.sector_weightings,
                func.sum(Share.amount).label("amount"),
                Price.amount.label("price"),
            )
            .distinct()
            .join(Share.portfolio_stocks)
            .join(PortfolioStocks.stock)
            .join(Stock.prices)
            .where(PortfolioStocks.portfolio_id == self.portfolio_id)
            .where(Price.date == self.date.strftime("%Y-%m-%d"))
            .group_by(Share.portfolio_stocks_id)
        )

    @property
    def db_data_df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "stock_type": "string",
            "sector_weightings": "object",
            "amount": "float64",
            "price": "float64",
        }

    @property
    def expand_by_sector_df(self) -> DataFrame:
        df = self.share_value_df.copy(deep=True)
        df = df.join(DataFrame(df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))

        return df.drop(columns=["sector_weightings"])

    @property
    def percent_by_moniker_df(self) -> DataFrame:
        df = self.expand_by_sector_df.copy(deep=True)
        df["total_value"] = df["value"].sum()
        df["percent"] = df["value"] / df["total_value"]

        return df.drop(columns=["value", "total_value"])

    @property
    def moniker_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)

        return df[["moniker", "percent"]]

    @property
    def stock_type_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)

        return df[["stock_type", "percent"]].groupby("stock_type").sum().reset_index()

    @property
    def sector_breakdown_df(self) -> DataFrame:
        df = self.percent_by_moniker_df.copy(deep=True)
        df = df.drop(columns=["moniker", "stock_type"])
        df_minus_percent = df.drop(columns="percent")
        df = df_minus_percent.multiply(df["percent"], axis="index")
        df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

        return df

    def write_json_files(self) -> None:
        assert self.output_dir is not None

        self.moniker_breakdown_df.to_json(self.output_dir / "moniker_breakdown.json", orient="records")
        self.stock_type_breakdown_df.to_json(self.output_dir / "stock_type_breakdown.json", orient="records")
        self.sector_breakdown_df.to_json(self.output_dir / "sector_breakdown.json", orient="records")

    def show(self) -> None:
        self.moniker_breakdown_df.plot.pie(
            y="percent", labels=self.moniker_breakdown_df["moniker"], figsize=(5, 5), autopct="%.2f%%"
        )
        self.stock_type_breakdown_df.plot.pie(
            y="percent", labels=self.stock_type_breakdown_df["stock_type"], figsize=(5, 5), autopct="%.2f%%"
        )
        self.sector_breakdown_df.plot.pie(
            y="percent", labels=self.sector_breakdown_df["sector"], figsize=(5, 5), autopct="%.2f%%"
        )

        plt.show()
