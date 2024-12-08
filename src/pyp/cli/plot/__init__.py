from pathlib import Path
from typing import Annotated, Optional

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer import Typer

from pyp.cli.plot.commands.breakdown import PlotBreakdown
from pyp.database.engine import engine
from pyp.database.models import Portfolio, User

plot_app = Typer(name="plot", help="Plot various charts of the portfolio.")


@plot_app.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio name.")],
) -> None:
    with Session(engine) as session:
        portfolio: Portfolio = session.scalars(
            select(Portfolio)
            .where(Portfolio.name == portfolio_name)
            .where(Portfolio.user.has(User.username == username))
        ).one()

        ctx.obj = {"portfolio_id": portfolio.id}


@plot_app.command(name="breakdown", help="Plot pie-charts of the portfolio breakdown.")
def breakdown(
    ctx: typer.Context,
    output_dir: Annotated[Optional[Path], typer.Option(help="The path to write the resulting json data files.")] = None,
) -> None:
    PlotBreakdown(ctx.obj["portfolio_id"], output_dir=output_dir).plot()


@plot_app.command(name="growth", help="Plot charts showing the portfolio growth.")
def growth(ctx: typer.Context) -> None:
    pass


@plot_app.command(name="growth-breakdown-mom", help="Plot charts showing the portfolio growth for month over month.")
def growth_breakdown_mom(ctx: typer.Context) -> None:
    pass


@plot_app.command(name="growth-breakdown", help="Plot charts showing the portfolio growth for moth over month.")
def growth_breakdown(ctx: typer.Context) -> None:
    pass
