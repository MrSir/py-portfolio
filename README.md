# py-portfolio
Investment Portfolio Visualizer with Python. [DEMO](https://codepen.io/MrSir88/full/xbKJjzx) of what a given portfolio with a number of equities looks like. 

## Features
The tool provides a simple mechanism for creating User's and Portfolios inside a locally stored sqlite database. The more noteworthy features are:
- Creation of User accounts
- Creation of multiple Portfolios per Account
- Creation of Currencies
- Addition of publicly traded equities to a Portfolio
- Ingestion of stock prices for a date range for all or sub set of the equities in the database via Yahoo Finance
- Ingestion of exchange rates between all currency pairs in the database via Currency API

## Installation
The `pyp` application requires the following be present in order to be setup:
- python 3.13
- poetry 1.8.4

Once the dependent software is in place in order to pull in the project dependencies you would run:
```console
poetry install
```

To confirm the project has all of its dependencies run:
```console
pyp --help
```

## Setup
The first thing you will need is to tell the tool to set up its database. This can be achieved by running:
```console
pyp setup
```
Optionally the command comes with a `--seed` or `-s` option that will seed the `USD`, `CAD`, and `EUR` currencies and the exchange rates between any pair of them for all of January 2021 to February 2025. No api needed, the rates are part of the repo.

### Exchange Rates
In order to allow the tool to pull in more exchange rates you need to provide an `.env` file in the root directory that contains a `FREE_CURRENCY_API_KEY` key inside it. Feel free to copy the `.env.sample` file and fill in the value of your own api key.

Once that is in place in order ingest exchange rates take a look at the following command:
```console
pyp ingest exchange-rates --help
```

> WARNING: depending on your API Key Tier there may be limitations to how much data you can pull per minute, so choose your date range carefully.

> NOTE: I do plan on expanding the pre-packaged exchange rates with the tool every so often up to the most recent completed month

### Creating Portfolio
Once the project is set up, and you have your exchange rates you are ready to start creating portfolios. In order to do this you will first need a user, which can be created by running:
```console
pyp user add <USERNAME>
```

Once you have a user created, to add a portfolio for that user run:
```console
pyp user add-portfolio <USERNAME> <PORTFOLIO_NAME>
```

Adding equities to your portfolio can be done by running:
```console
pyp portfolio <USERNAME> <PORTFOLIO_NAME> add <MONIKER>
```

And finally adding your share holdings for the equities in your portfolio can be done by running:
```console
pyp portfolio <USERNAME> <PORTFOLIO_NAME> add-shares <MONIKER> <AMOUNT> <PRICE> <PURCHASED_ON>
```
Similarly, removing (selling) share holdings can be done by:
```console
pyp portfolio <USERNAME> <PORTFOLIO_NAME> remove-shares <MONIKER> <AMOUNT> <PRICE> <PURCHASED_ON>
```

> NOTE: the <PRICE> is the $/share you paid and not the total amount you paid for all the shares purchased at that time.

### Market Prices
Now that you have a portfolio of a number of equities, you need to grab the market prices for them in order to visualize your portfolio. Take a look at the following command for ways to do that:
```console
pyp ingest stocks --help
```

> NOTE: by default the command will fetch the data for the last 12 months for all equities found in the database for all users and portfolios. The parameters on the command will allow you to optimize for date ranges and specific monikers.

## Visualizing the Portfolio
Finally, in order to view a snapshot of your portfolio use the following command:
```console
pyp output <USERNAME> <PORTFOLIO_NAME>
```

The command supports a `--date` or `-d` option in order to specify the exact date you want the snapshot for. By default, it uses today.

The command supports a `--currency` or `-c` option in order to specify the Currency you want the snapshot for in. By default, it uses `USD`. The output command will automatically detect which equities are traded in what currencies and convert them to the requested currency.

The output command will generate the necessary javascript data files inside the `/public/js/output` directory. Once they are in place simply open the `/public/profile.html` file in a browser to view your snapshot.