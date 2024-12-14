from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer
from typer import Typer

from pyp.cli.common import resolve_portfolio
from pyp.cli.plot.commands.breakdown import PlotBreakdown
from pyp.cli.plot.commands.growth import PlotGrowth as PlotGrowth
from pyp.cli.plot.commands.growth_breakdown import PlotGrowthBreakdown, PlotGrowthBreakdownMonthOverMonth

plot_app = Typer(name="plot", help="Plot various charts of the portfolio.")


@plot_app.callback()
def callback(
    ctx: typer.Context,
    username: Annotated[str, typer.Argument(help="The username of the user.")],
    portfolio_name: Annotated[str, typer.Argument(help="The portfolio name.")],
    date: Annotated[
        datetime, typer.Option("--date", "-d", metavar="YYYY-MM-DD", help="The date to plot for.")
    ] = datetime.now(),
) -> None:
    ctx.obj = {"date": date, "portfolio_id": resolve_portfolio(username, portfolio_name).id}


@plot_app.command(name="breakdown", help="Plot pie-charts of the portfolio breakdown.")
def breakdown(
    ctx: typer.Context,
    output_dir: Annotated[Optional[Path], typer.Option(help="The path to write the resulting json data files.")] = None,
) -> None:
    PlotBreakdown(ctx.obj["portfolio_id"], ctx.obj["date"], output_dir=output_dir).execute()


@plot_app.command(name="growth", help="Plot charts showing the overall portfolio growth.")
def growth(
    ctx: typer.Context,
    output_dir: Annotated[Optional[Path], typer.Option(help="The path to write the resulting json data files.")] = None,
) -> None:
    PlotGrowth(ctx.obj["portfolio_id"], ctx.obj["date"], output_dir=output_dir).execute()


@plot_app.command(name="growth-breakdown", help="Plot charts showing the portfolio growth breakdown.")
def growth_breakdown(
    ctx: typer.Context,
    month_over_month: Annotated[
        Optional[bool],
        typer.Option(
            "--month-over-month",
            "-mom",
            help="Plot the month over month growth breakdown instead.",
        ),
    ] = False,
    output_dir: Annotated[Optional[Path], typer.Option(help="The path to write the resulting json data files.")] = None,
) -> None:
    if month_over_month:
        PlotGrowthBreakdownMonthOverMonth(ctx.obj["portfolio_id"], ctx.obj["date"], output_dir=output_dir).execute()

        return

    PlotGrowthBreakdown(ctx.obj["portfolio_id"], ctx.obj["date"], output_dir=output_dir).execute()
