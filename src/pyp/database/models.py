from datetime import date, datetime

from sqlalchemy import Date, Double, Enum, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(256), unique=True)

    portfolios: Mapped[list["Portfolio"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    #
    # def __repr__(self) -> str:
    #     return f"User(id={self.id!r}, name={self.username!r})"


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(256))

    user: Mapped["User"] = relationship(back_populates="portfolios")
    stocks: Mapped[list["Stock"]] = relationship(secondary="portfolio_stocks", back_populates="portfolios")
    portfolio_stocks: Mapped[list["PortfolioStocks"]] = relationship(back_populates="portfolio", viewonly=True)

    __table_args__ = (UniqueConstraint("name", "user_id", name="portfolio_constraint"),)
    #
    # def __repr__(self) -> str:
    #     return f"Portfolio(id={self.id!r}, user_id={self.user_id!r}, name={self.name!r})"


class Stock(Base):
    __tablename__ = "stocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    currency_id: Mapped[int] = mapped_column(ForeignKey("currencies.id"), nullable=True)
    stock_type: Mapped[str] = mapped_column(Enum("ETF", "EQUITY"), nullable=True)
    moniker: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=True)
    description: Mapped[str] = mapped_column(String(256), nullable=True)
    sector_weightings: Mapped[str] = mapped_column(String(256), nullable=True)

    currency: Mapped["Currency"] = relationship(back_populates="stocks")
    portfolios: Mapped[list["Portfolio"]] = relationship(secondary="portfolio_stocks", back_populates="stocks")
    portfolio_stocks: Mapped[list["PortfolioStocks"]] = relationship(back_populates="stock", viewonly=True)
    prices: Mapped[list["Price"]] = relationship(back_populates="stock", cascade="all, delete-orphan")
    #
    # def __repr__(self) -> str:
    #     return f"Stock(id={self.id!r}, moniker={self.moniker!r}, name={self.name!r})"


class PortfolioStocks(Base):
    __tablename__ = "portfolio_stocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    portfolio_id: Mapped[int] = mapped_column(ForeignKey("portfolios.id"))
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"))

    portfolio: Mapped["Portfolio"] = relationship(back_populates="portfolio_stocks", viewonly=True)
    stock: Mapped["Stock"] = relationship(back_populates="portfolio_stocks", viewonly=True)
    shares: Mapped[list["Share"]] = relationship(back_populates="portfolio_stocks", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("portfolio_id", "stock_id", name="p_s_index"),)
    #
    # def __repr__(self) -> str:
    #     return f"PortfolioStock(id={self.id!r}, portfolio_id={self.portfolio_id!r}, stock_id={self.stock_id!r})"


class Share(Base):
    __tablename__ = "shares"

    id: Mapped[int] = mapped_column(primary_key=True)
    portfolio_stocks_id: Mapped[int] = mapped_column(ForeignKey("portfolio_stocks.id"))
    amount: Mapped[float] = mapped_column(Double())
    price: Mapped[float] = mapped_column(Double())
    purchased_on: Mapped[date] = mapped_column(Date())

    portfolio_stocks: Mapped["PortfolioStocks"] = relationship(back_populates="shares", viewonly=True)
    #
    # def __repr__(self) -> str:
    #     return (
    #         f"Shares(id={self.id!r}, portfolio_stocks_id={self.portfolio_stocks_id!r}, amount={self.amount!r}, "
    #         f"price={self.price!r}, purchased_on={self.purchased_on!r})"
    #     )


class Price(Base):
    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"))
    date: Mapped[datetime] = mapped_column(Date())
    amount: Mapped[float] = mapped_column(Double())

    stock: Mapped["Stock"] = relationship(back_populates="prices")

    __table_args__ = (UniqueConstraint("stock_id", "date", name="s_d_index"),)
    #
    # def __repr__(self) -> str:
    #     return f"Price(id={self.id!r}, stock_id={self.stock_id!r}, date={self.date!r}, amount={self.amount!r})"


class Currency(Base):
    __tablename__ = "currencies"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(5), unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=True)

    stocks: Mapped[list["Stock"]] = relationship(back_populates="currency", cascade="all, delete-orphan")
    #
    # def __repr__(self) -> str:
    #     return f"Currency(id={self.id!r}, code={self.code!r}, full_name={self.name!r})"


class ExchangeRate(Base):
    __tablename__ = "exchange_rates"

    id: Mapped[int] = mapped_column(primary_key=True)
    from_currency_id: Mapped[int] = mapped_column(ForeignKey("currencies.id"))
    to_currency_id: Mapped[int] = mapped_column(ForeignKey("currencies.id"))
    date: Mapped[datetime] = mapped_column(Date())
    rate: Mapped[float] = mapped_column(Double())

    from_currency: Mapped["Currency"] = relationship("Currency", foreign_keys=[from_currency_id])
    to_currency: Mapped["Currency"] = relationship("Currency", foreign_keys=[to_currency_id])

    __table_args__ = (UniqueConstraint("from_currency_id", "to_currency_id", "date", name="from_to_date_index"),)
    #
    # def __repr__(self) -> str:
    #     return (
    #         "ExchangeRate("
    #         + "id={self.id!r}, from={self.from_currency_id!r}, to={self.to_currency_id!r}, rate={self.rate!r}"
    #         + ")"
    #     )
