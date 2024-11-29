from typer import Typer

from .ingest import ingest
from .plot import plot
from .portfolio import portfolio
from .user import user

app = Typer()
app.add_typer(ingest)
app.add_typer(plot)
app.add_typer(user)
app.add_typer(portfolio)

if __name__ == "__main__":
    app()
