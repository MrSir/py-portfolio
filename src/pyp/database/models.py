import enum
from datetime import datetime

from sqlalchemy import String, ForeignKey, UniqueConstraint, Date, Column, Double, Enum
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
    name: Mapped[str] = mapped_column(String(256))

    user: Mapped["User"] = relationship(back_populates="portfolios")
    stocks: Mapped[list["Stock"]] = relationship(secondary="portfolio_stocks", back_populates="portfolios")
    portfolio_stocks: Mapped[list["PortfolioStocks"]] = relationship(back_populates="portfolio", viewonly=True)

    __table_args__ = (UniqueConstraint("name", "user_id", name="portfolio_constraint"),)

    def __repr__(self) -> str:
        return f"Portfolio(id={self.id!r}, user_id={self.user_id!r}, name={self.name!r})"


class Stock(Base):
    __tablename__ = "stocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    currency_id: Mapped[int] = mapped_column(ForeignKey("currencies.id"), nullable=True)
    stock_type: Mapped[str] = mapped_column(Enum("ETF", "Stock", "REIT"), nullable=True)
    moniker: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=True)
    description: Mapped[str] = mapped_column(String(256), nullable=True)
    sector_weightings: Mapped[dict] = mapped_column(String(256), nullable=True)

    currency: Mapped["Currency"] = relationship(back_populates="stocks")
    portfolios: Mapped[list["Portfolio"]] = relationship(secondary="portfolio_stocks", back_populates="stocks")
    portfolio_stocks: Mapped[list["PortfolioStocks"]] = relationship(back_populates="stock", viewonly=True)
    prices: Mapped[list["Price"]] = relationship(back_populates="stock", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"Stock(id={self.id!r}, moniker={self.moniker!r}, name={self.name!r})"


class PortfolioStocks(Base):
    __tablename__ = "portfolio_stocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    portfolio_id: Mapped[int] = mapped_column(ForeignKey("portfolios.id"))
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"))

    portfolio: Mapped["Portfolio"] = relationship(back_populates="portfolio_stocks", viewonly=True)
    stock: Mapped["Stock"] = relationship(back_populates="portfolio_stocks", viewonly=True)

    __table_args__ = (UniqueConstraint("portfolio_id", "stock_id", name="p_s_index"),)

    def __repr__(self) -> str:
        return f"PortfolioStock(id={self.id!r}, portfolio_id={self.portfolio_id!r}, stock_id={self.stock_id!r})"


class Price(Base):
    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"))
    date: Mapped[datetime] = mapped_column(Date())
    amount: Mapped[float] = mapped_column(Double())

    stock: Mapped["Stock"] = relationship(back_populates="prices")

    __table_args__ = (UniqueConstraint("stock_id", "date", name="s_d_index"),)


class Currency(Base):
    __tablename__ = "currencies"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(5), unique=True)
    full_name: Mapped[str] = mapped_column(String(256), nullable=True)

    stocks: Mapped[list["Stock"]] = relationship(back_populates="currency", cascade="all, delete-orphan")