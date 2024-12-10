from pyp.cli.plot.commands.growth import PlotGrowth


def test_initialization() -> None:
    command = PlotGrowth()

    assert isinstance(command, PlotGrowth)
