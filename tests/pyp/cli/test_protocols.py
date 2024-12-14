from pyp.cli.protocols import CommandProtocol, PlotCommandProtocol


def test_command_protocol() -> None:
    assert hasattr(CommandProtocol, "execute")


def test_plot_command_protocol() -> None:
    assert issubclass(PlotCommandProtocol, CommandProtocol)
    assert hasattr(PlotCommandProtocol, "_db_query")
    assert hasattr(PlotCommandProtocol, "_df_dtypes")
    assert hasattr(PlotCommandProtocol, "_prepare_df")
    assert hasattr(PlotCommandProtocol, "_write_json_files")
    assert hasattr(PlotCommandProtocol, "_show")
