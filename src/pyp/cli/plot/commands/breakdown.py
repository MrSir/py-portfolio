import json
from typing import Self

from matplotlib import pyplot as plt
from pandas import DataFrame
from sqlalchemy import Selectable, func, select

from pyp.cli.plot.commands.base import PlotCommand
from pyp.database.models import PortfolioStocks, Price, Share, Stock


class PlotBreakdown(PlotCommand):
    @property
    def _db_query(self) -> Selectable:
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
    def _df_dtypes(self) -> dict[str, str]:
        return {
            "moniker": "string",
            "stock_type": "string",
            "sector_weightings": "object",
            "amount": "float64",
            "price": "float64",
        }

    def _expand_by_sector(self) -> Self:
        if self._df is not None:
            self._df["sector_weightings"] = self._df["sector_weightings"].str.replace("_", "-")

            self._df = (
                self._df.join(DataFrame(self._df["sector_weightings"].apply(json.loads).tolist()).fillna(0.0))
            ).drop(columns=["amount", "sector_weightings"])

        return self

    def _calculate_percent_by_moniker(self) -> Self:
        if self._df is not None:
            self._df["total_value"] = self._df["value"].sum()
            self._df["percent"] = self._df["value"] / self._df["total_value"]
            self._df = self._df.drop(columns=["value", "total_value"])

        return self

    @property
    def _moniker_breakdown_df(self) -> DataFrame:
        return self._df[["moniker", "percent"]]  # type:ignore[index]

    @property
    def _stock_type_breakdown_df(self) -> DataFrame:
        return self._df[["stock_type", "percent"]].groupby("stock_type").sum().reset_index()  # type:ignore[index]

    @property
    def _sector_breakdown_df(self) -> DataFrame:
        df = self._df.drop(columns=["moniker", "stock_type"])  # type:ignore[union-attr]
        df = df.drop(columns="percent").multiply(df["percent"], axis="index")
        df = df.sum().to_frame().reset_index().rename(columns={"index": "sector", 0: "percent"})

        return df

    def _prepare_df(self) -> None:
        self._read_db()._compute_share_value()._expand_by_sector()._calculate_percent_by_moniker()

    def _write_json_files(self) -> None:
        if self.output_dir is not None:
            self._moniker_breakdown_df.to_json(self.output_dir / "moniker_breakdown.json", orient="records")
            self._stock_type_breakdown_df.to_json(self.output_dir / "stock_type_breakdown.json", orient="records")
            self._sector_breakdown_df.to_json(self.output_dir / "sector_breakdown.json", orient="records")

    def _show(self) -> None:
        self._moniker_breakdown_df.plot.pie(
            title="Percent of Portfolio by Moniker",
            y="percent",
            labels=self._moniker_breakdown_df["moniker"],
            figsize=(5, 5),
            autopct="%.2f%%",
            legend=False,
        )
        self._stock_type_breakdown_df.plot.pie(
            title="Percent of Portfolio by Stock Type",
            y="percent",
            labels=self._stock_type_breakdown_df["stock_type"],
            figsize=(5, 5),
            autopct="%.2f%%",
            legend=False,
        )
        self._sector_breakdown_df.plot.pie(
            title="Percent of Portfolio by Sector",
            y="percent",
            labels=self._sector_breakdown_df["sector"],
            figsize=(5, 5),
            autopct="%.2f%%",
            legend=False,
        )

        plt.show()
