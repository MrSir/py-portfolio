from typer import Typer

user = Typer(name="user", help="Manage user DB entities.")


@user.command(
    name="create",
    help="Creates a new user in the system."
)
def create() -> None:
    pass
