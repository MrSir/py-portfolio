from typing import Annotated

import typer
from sqlalchemy.orm import Session
from typer import Typer

from pyp.database.engine import engine
from pyp.database.models import User

user_app = Typer(name="user", help="Manage user DB entities.")


@user_app.command(name="create", help="Creates a new user in the system.")
def create(username: Annotated[str, typer.Argument(help="The username of the user.")],) -> None:
    with Session(engine) as session:
        user = User(username=username)

        session.add(user)

        session.commit()
