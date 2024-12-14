from datetime import datetime
from pathlib import Path

from pyp.cli.plot.commands.base import PlotCommand
from pyp.cli.plot.commands.growth_breakdown import PlotGrowthBreakdownMonthOverMonth
from pyp.cli.protocols import PlotCommandProtocol


def test_initialization(portfolio_id: int) -> None:
    output_dir = Path(__file__).parent
    date = datetime(2024, 12, 9)

    command = PlotGrowthBreakdownMonthOverMonth(portfolio_id, date, output_dir=output_dir)

    assert isinstance(command, PlotCommand)
    assert isinstance(command, PlotCommandProtocol)
