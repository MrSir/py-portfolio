from typer import Typer

plot = Typer(name="plot", help="Plot various charts of the portfolio.")


@plot.command(name="breakdown", help="Plot pie-charts of the portfolio breakdown.")
def breakdown() -> None:
    pass


@plot.command(name="growth", help="Plot charts showing the portfolio growth.")
def growth() -> None:
    pass


@plot.command(name="growth-breakdown-mom", help="Plot charts showing the portfolio growth for month over month.")
def growth_breakdown_mom() -> None:
    pass


@plot.command(name="growth-breakdown", help="Plot charts showing the portfolio growth for moth over month.")
def growth_breakdown() -> None:
    pass
