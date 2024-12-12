from pyp.cli.plot.commands.protocols import PlotCommandProtocol


def test_plot_command_protocol() -> None:
    assert hasattr(PlotCommandProtocol, "_db_query")
    assert hasattr(PlotCommandProtocol, "_df_dtypes")
    assert hasattr(PlotCommandProtocol, "_prepare_df")
    assert hasattr(PlotCommandProtocol, "_write_json_files")
    assert hasattr(PlotCommandProtocol, "_show")
