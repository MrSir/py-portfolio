from typing import Self

from pandas import DataFrame

from pyp.cli.commands.output.growth import OutputGrowthCommand


class OutputGrowthBreakdownCommand(OutputGrowthCommand):
    def _compute_cumulative_sums(self) -> Self:
        self._df["cum_sum_invested"] = self._df.groupby(["moniker"], sort=False)["invested"].cumsum()

        self._df = self._df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

        return self

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._rename_value_to_invested()
            ._sum_up_by_month_and_moniker()
            ._add_monthly_market_prices()
            ._compute_market_value()
            ._resolve_currency_ids()
            ._add_exchange_rates()
            ._convert_to_currency()
            ._compute_cumulative_sums()
            ._compute_profit()
            ._compute_profit_ratio()
            ._compute_profit_ratio_difference()
        )

    @property
    def _equity_month_vs_profit_ratio_df(self) -> DataFrame:
        df = self._df[self._df["stock_type"] == "EQUITY"].copy(deep=True)
        df = df[["month", "moniker", "profit_ratio"]]

        data = {
            "month": df["month"].unique(),
        }

        for moniker in df["moniker"].unique():
            data[moniker] = df[df["moniker"] == moniker]["profit_ratio"].to_list()

        return DataFrame(data=data)

    @property
    def _etf_month_vs_profit_ratio_df(self) -> DataFrame:
        df = self._df[self._df["stock_type"] == "ETF"].copy(deep=True)
        df = df[["month", "moniker", "profit_ratio"]]

        data = {
            "month": df["month"].unique(),
        }

        for moniker in df["moniker"].unique():
            data[moniker] = df[df["moniker"] == moniker]["profit_ratio"].to_list()

        return DataFrame(data=data)

    def _write_data_files(self) -> None:
        equity_json_string = self._equity_month_vs_profit_ratio_df.to_json(orient="records")
        etf_json_string = self._etf_month_vs_profit_ratio_df.to_json(orient="records")

        growth_breakdown_by_stock_type = f'{{"EQUITY": {equity_json_string}, "ETF": {etf_json_string}}}'

        with open(self.output_dir / "breakdown.js", "a") as file:
            file.write(f"growth_breakdown_by_stock_type_data = {growth_breakdown_by_stock_type}\n")


class OutputGrowthBreakdownMonthOverMonthCommand(OutputGrowthBreakdownCommand):
    def _convert_to_currency(self) -> Self:
        self._df["invested"] = self._df["invested"] * self._df["rate"]
        self._df["market_price"] = self._df["market_price"] * self._df["rate"]

        self._df = self._df.drop(columns=["rate"])

        return self

    def _calculate_monthly_difference(self) -> Self:
        self._df["price_difference"] = self._df.groupby(["moniker"], sort=False)["market_price"].diff().fillna(0)
        self._df["last_market_price"] = self._df.groupby(["moniker"], sort=False)["market_price"].shift().fillna(0)
        self._df["month_over_month_ratio"] = (self._df["price_difference"] / self._df["last_market_price"]).fillna(0)
        self._df = self._df.drop(columns=["price_difference", "last_market_price"])

        return self

    def _prepare_df(self) -> None:
        (
            self._read_db()
            ._compute_share_value()
            ._rename_value_to_invested()
            ._sum_up_by_month_and_moniker()
            ._add_monthly_market_prices()
            ._resolve_currency_ids()
            ._add_exchange_rates()
            ._convert_to_currency()
            ._calculate_monthly_difference()
        )

    @property
    def _equity_month_over_month_ratio_df(self) -> DataFrame:
        df = self._df[self._df["stock_type"] == "EQUITY"].copy(deep=True)
        df = df[["month", "moniker", "month_over_month_ratio"]]

        data = {
            "month": df["month"].unique(),
        }

        for moniker in df["moniker"].unique():
            data[moniker] = df[df["moniker"] == moniker]["month_over_month_ratio"].to_list()

        return DataFrame(data=data)

    @property
    def _etf_month_over_month_ratio_df(self) -> DataFrame:
        df = self._df[self._df["stock_type"] == "ETF"].copy(deep=True)
        df = df[["month", "moniker", "month_over_month_ratio"]]

        data = {
            "month": df["month"].unique(),
        }

        for moniker in df["moniker"].unique():
            data[moniker] = df[df["moniker"] == moniker]["month_over_month_ratio"].to_list()

        return DataFrame(data=data)

    def _write_data_files(self) -> None:
        equity_json_string = self._equity_month_over_month_ratio_df.to_json(orient="records")
        etf_json_string = self._etf_month_over_month_ratio_df.to_json(orient="records")

        growth_breakdown_by_stock_type = f'{{"EQUITY": {equity_json_string}, "ETF": {etf_json_string}}}'

        with open(self.output_dir / "breakdown.js", "a") as file:
            file.write(f"growth_breakdown_mom_by_stock_type_data = {growth_breakdown_by_stock_type}\n")
