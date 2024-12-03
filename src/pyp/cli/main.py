from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import Base
from .currencies import currency_app
from .ingest import ingest
from .plot import plot
from .portfolio import portfolio_app
from .user import user_app

app = Typer()
app.add_typer(ingest)
app.add_typer(plot)
app.add_typer(currency_app)
app.add_typer(user_app)
app.add_typer(portfolio_app)

if __name__ == "__main__":
    app()


@app.command(name="setup", help="Creates the database and sets up the project.")
def setup() -> None:
    Base.metadata.create_all(engine)
