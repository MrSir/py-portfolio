from datetime import datetime

import pytest
from pandas import DataFrame


@pytest.fixture
def share_value_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": ["ADP", "ADP", "ADP", "IYK", "IYK", "IYK", "RDVY", "AP-UN.TO"],
            "stock_type": ["EQUITY", "EQUITY", "EQUITY", "ETF", "ETF", "ETF", "ETF", "EQUITY"],
            "amount": [1.749, 0.009, 0.008, 4.795, 1.000, 0.864, 19.065, 10],
            "value": [432.107940, 2.092270, 1.970961, 863.690744, 187.400000, 162.215136, 863.545362, 189.90],
            "month": [
                "2023-10",
                "2024-01",
                "2024-04",
                "2023-10",
                "2023-11",
                "2023-11",
                "2023-10",
                "2023-10",
            ],
        }
    ).astype(
        dtype={
            "moniker": "string",
            "stock_type": "string",
            "amount": "float64",
            "value": "float64",
            "month": "string",
        }
    )


@pytest.fixture
def invested_df(share_value_df: DataFrame) -> DataFrame:
    df = share_value_df.copy(deep=True)
    return df.rename(columns={"value": "invested"})


@pytest.fixture
def monthly_df(invested_df: DataFrame) -> DataFrame:
    df = invested_df.copy(deep=True)

    return (
        df.groupby(["moniker", "month"], sort=False)
        .agg({
            "stock_type": "first",
            "amount": "sum",
            "invested": "sum",
        })
        .reset_index()
    )


@pytest.fixture
def monthly_prices_df() -> DataFrame:
    return DataFrame(
        data={
            "moniker": [
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "ADP",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "IYK",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "RDVY",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
                "AP-UN.TO",
            ],
            "stock_type": [
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "ETF",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
                "EQUITY",
            ],
            "currency_id": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
            ],
            "month": [
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
            ],
            "market_price": [
                213.34510803222656,
                224.78372192382812,
                229.14627075195312,
                241.74603271484375,
                247.0082244873047,
                247.0599822998047,
                239.29421997070312,
                242.2917022705078,
                237.4897003173828,
                261.2993469238281,
                274.52252197265625,
                276.7300109863281,
                289.239990234375,
                306.92999267578125,
                296.9599914550781,
                59.6072998046875,
                61.84817123413086,
                62.784698486328125,
                63.33489990234375,
                64.09796905517578,
                66.72958374023438,
                65.98956298828125,
                65.97969818115234,
                65.26890563964844,
                67.4031753540039,
                70.27202606201172,
                70.56999969482422,
                68.31999969482422,
                70.91999816894531,
                68.60919952392578,
                43.41899490356445,
                47.36438751220703,
                51.04840087890625,
                50.73225402832031,
                52.20433044433594,
                55.718299865722656,
                52.89122009277344,
                54.80569839477539,
                54.57081604003906,
                58.268672943115234,
                58.55772399902344,
                59.209999084472656,
                59.040000915527344,
                64.29000091552734,
                62.4119987487793,
                13.88,
                15.78,
                18.39,
                17.76,
                15.59,
                16.36,
                15.81,
                15.92,
                14.57,
                15.88,
                16.83,
                19.72,
                18.06,
                18.01,
                17.15,
            ],
        }
    ).astype(
        dtype={
            "moniker": "string",
            "stock_type": "string",
            "currency_id": "int64",
            "month": "string",
            "market_price": "float64",
        }
    )


@pytest.fixture
def added_monthly_market_prices_df(monthly_prices_df: DataFrame, monthly_df: DataFrame) -> DataFrame:
    df = monthly_prices_df.merge(
        monthly_df,
        how="left",
        left_on=["moniker", "month", "stock_type"],
        right_on=["moniker", "month", "stock_type"],
    )

    df[["amount", "invested"]] = df[["amount", "invested"]].fillna(0, axis="columns")
    df["amount"] = df.groupby("moniker", sort=False)["amount"].cumsum()

    df["month"] = df["month"].apply(lambda x: datetime.strptime(x, "%Y-%m").strftime("%b-%y"))

    return df


@pytest.fixture
def market_value_df(added_monthly_market_prices_df: DataFrame) -> DataFrame:
    df = added_monthly_market_prices_df.copy(deep=True)
    df["value"] = df["amount"] * df["market_price"]

    df = df.drop(columns=["amount", "market_price"])

    return df


@pytest.fixture
def exchange_rates_df() -> DataFrame:
    return DataFrame(
        data={
            "from_currency_id": [
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
            ],
            "to_currency_id": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            ],
            "month": [
                "2022-01",
                "2022-02",
                "2022-03",
                "2022-04",
                "2022-05",
                "2022-06",
                "2022-07",
                "2022-08",
                "2022-09",
                "2022-10",
                "2022-11",
                "2022-12",
                "2023-01",
                "2023-02",
                "2023-03",
                "2023-04",
                "2023-05",
                "2023-06",
                "2023-07",
                "2023-08",
                "2023-09",
                "2023-10",
                "2023-11",
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2025-01",
            ],
            "rate": [
                0.7872713961,
                0.7894217486,
                0.7999936001,
                0.7786039631,
                0.7911329815,
                0.7767231603,
                0.7802417813,
                0.7609052947,
                0.7229058502,
                0.7345174734,
                0.745505904,
                0.7369457431,
                0.7516327342,
                0.7327870162,
                0.7392884497,
                0.7374756356,
                0.7370570486,
                0.7549725858,
                0.7580982851,
                0.740060864,
                0.7365016523,
                0.7206011762,
                0.7377404614,
                0.7548422249,
                0.7443299812,
                0.7368435243,
                0.7395792311,
                0.7259737541,
                0.7336540808,
                0.7314110919,
                0.7243016606,
                0.7410974824,
                0.7394424697,
                0.7177616402,
                0.714275425,
                0.6953183353,
                0.6926645828,
            ],
        }
    ).astype(
        dtype={
            "from_currency_id": "int64",
            "to_currency_id": "int64",
            "month": "string",
            "rate": "float64",
        }
    )


@pytest.fixture
def formatted_exchange_rates_df(exchange_rates_df: DataFrame) -> DataFrame:
    df = exchange_rates_df.copy(deep=True)

    df["month"] = df["month"].apply(lambda x: datetime.strptime(x, "%Y-%m").strftime("%b-%y"))

    return df


@pytest.fixture
def with_exchange_rates_df(market_value_df: DataFrame, formatted_exchange_rates_df: DataFrame) -> DataFrame:
    df = market_value_df.copy(deep=True)
    df = df.merge(
        formatted_exchange_rates_df,
        how="left",
        left_on=["month", "currency_id"],
        right_on=["month", "from_currency_id"],
    )

    df["rate"] = df["rate"].fillna(1)

    df = df.drop(columns=["from_currency_id", "to_currency_id", "currency_id"])

    return df


@pytest.fixture
def converted_to_currency(with_exchange_rates_df: DataFrame) -> DataFrame:
    df = with_exchange_rates_df.copy(deep=True)
    df["invested"] = df["invested"] * df["rate"]
    df["value"] = df["value"] * df["rate"]

    df = df.drop(columns=["rate"])

    return df


@pytest.fixture
def summed_monthly_df(converted_to_currency: DataFrame) -> DataFrame:
    df = converted_to_currency.copy(deep=True)

    return (
        df.groupby(["month"], sort=False)
        .agg({
            "stock_type": "first",
            "invested": "sum",
            "value": "sum",
        })
        .reset_index()
    )


@pytest.fixture
def cumulative_sum_df(summed_monthly_df: DataFrame) -> DataFrame:
    df = summed_monthly_df.copy(deep=True)

    df["cum_sum_invested"] = df["invested"].cumsum()

    df = df.drop(columns=["invested"]).rename(columns={"cum_sum_invested": "invested"})

    return df


@pytest.fixture
def profit_df(cumulative_sum_df: DataFrame) -> DataFrame:
    df = cumulative_sum_df.copy(deep=True)
    df["profit"] = df["value"] - df["invested"]

    return df


@pytest.fixture
def profit_ratio_df(profit_df: DataFrame) -> DataFrame:
    df = profit_df.copy(deep=True)
    df["profit_ratio"] = (df["profit"] / df["invested"]).fillna(0)

    return df


@pytest.fixture
def profit_ratio_difference_df(profit_ratio_df: DataFrame) -> DataFrame:
    df = profit_ratio_df.copy(deep=True)
    df["profit_ratio_difference"] = df["profit_ratio"] - df["profit_ratio"].shift()

    return df
