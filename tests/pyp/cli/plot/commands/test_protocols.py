from pyp.cli.plot.commands.protocols import PlotCommandProtocol


def test_plot_command_protocol() -> None:
    assert hasattr(PlotCommandProtocol, "db_query")
    assert hasattr(PlotCommandProtocol, "db_data_df_dtypes")
