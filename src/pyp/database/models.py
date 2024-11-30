from sqlalchemy import String, ForeignKey, UniqueConstraint, Table, Column
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(256), unique=True)

    portfolios: Mapped[list["Portfolio"]] = relationship(back_populates="user", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.username!r})"


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped["User"] = relationship(back_populates="portfolios")
    name: Mapped[str] = mapped_column(String(256))

    stocks: Mapped[list["Stock"]] = relationship(secondary="portfolio_stocks", back_populates="portfolios")
    portfolio_stocks: Mapped[list["PortfolioStocks"]] = relationship(back_populates="portfolio")

    __table_args__ = (UniqueConstraint("name", "user_id", name="portfolio_constraint"),)

    def __repr__(self) -> str:
        return f"Portfolio(id={self.id!r}, user_id={self.user_id!r}, name={self.name!r})"


class PortfolioStocks(Base):
    __tablename__ = "portfolio_stocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    portfolio_id: Mapped[int] = mapped_column(ForeignKey("portfolios.id"))
    portfolio: Mapped["Portfolio"] = relationship(back_populates="portfolio_stocks")
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"))
    stock: Mapped["Stock"] = relationship(back_populates="portfolio_stocks")

    __table_args__ = (UniqueConstraint("portfolio_id", "stock_id", name="p_s_index"),)

    def __repr__(self) -> str:
        return f"PortfolioStock(id={self.id!r}, portfolio_id={self.portfolio_id!r}, stock_id={self.stock_id!r})"


class Stock(Base):
    __tablename__ = "stocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    moniker: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=True)
    description: Mapped[str] = mapped_column(String(256), nullable=True)

    portfolios: Mapped[list["Portfolio"]] = relationship(secondary="portfolio_stocks", back_populates="stocks")
    portfolio_stocks: Mapped[list["PortfolioStocks"]] = relationship(back_populates="stock")

    def __repr__(self) -> str:
        return f"Stock(id={self.id!r}, moniker={self.moniker!r}, name={self.name!r})"
