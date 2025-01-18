from pyp.cli.protocols import CommandProtocol, OutputCommandProtocol


def test_command_protocol() -> None:
    assert hasattr(CommandProtocol, "execute")


def test_plot_command_protocol() -> None:
    assert issubclass(OutputCommandProtocol, CommandProtocol)
    assert hasattr(OutputCommandProtocol, "_db_query")
    assert hasattr(OutputCommandProtocol, "_df_dtypes")
    assert hasattr(OutputCommandProtocol, "_prepare_df")
    assert hasattr(OutputCommandProtocol, "_write_data_files")
