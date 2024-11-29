from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(256), unique=True)

    portfolios: Mapped[list["Portfolio"]] = relationship(back_populates = "user", cascade = "all, delete-orphan")

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.username!r})"


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped["User"] = relationship(back_populates="portfolios")
    name: Mapped[str] = mapped_column(String(256))

    def __repr__(self) -> str:
        return f"Portfolio(id={self.id!r}, user_id={self.user_id!r}, name={self.name!r})"
