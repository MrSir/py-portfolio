from typing import Annotated

import typer
from sqlalchemy.orm import Session
from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import Base, Currency

from .currencies import currency_app
from .ingest import ingest
from .plot import plot_app
from .portfolio import portfolio_app
from .user import user_app

app = Typer()
app.add_typer(ingest)
app.add_typer(plot_app)
app.add_typer(currency_app)
app.add_typer(user_app)
app.add_typer(portfolio_app)

if __name__ == "__main__":
    app()


@app.command(name="setup", help="Creates the database and sets up the project.")
def setup(
    seed: Annotated[
        bool,
        typer.Option("--seed", "-s", help="Seed the database with common records."),
    ] = False,
) -> None:
    Base.metadata.create_all(engine)

    if seed:
        with Session(engine) as session:
            currencies = [
                {"name": "USD", "full_name": "United States Dollar"},
                {"name": "CAD", "full_name": "Canadian Dollar"},
                {"name": "EUR", "full_name": "Euro"},
            ]

            for c in currencies:
                session.add(Currency(**c))

            session.commit()
