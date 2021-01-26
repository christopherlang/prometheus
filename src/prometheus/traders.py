import abc
import pytz
import random
import functools
import asyncio
import json
import numpy
import pandas
from prometheus import CFG
from prometheus import utils
from prometheus.datasource import api
from prometheus import brokerage


def check_has_any_stocks(fn):

    @functools.wraps(fn)
    async def check_has_any_stocks_decorated(self, *args, **kwargs):
        if self.has_any_stock is False:
            raise ValueError('No stocks were added to trader')
        else:
            return await fn(self, *args, **kwargs)

    return check_has_any_stocks_decorated


def interval_event_loop(fn):

    @functools.wraps(fn)
    async def interval_event_loop_decorated(self, *args, **kwargs):

        while True:
            if self.is_active is True:
                await asyncio.sleep(self._execution_freq)
                fn(self, *args, **kwargs)
            else:
                break

    return interval_event_loop_decorated


class StockTrader(metaclass=abc.ABCMeta):

    def __init__(self, brokerage: brokerage.Brokerage,
                 data_source: api.DataSource, timezone: str = 'EST',
                 is_day_trader: bool = False, cash_value=10000):

        self._timezone = pytz.timezone(timezone)
        self._portfolio = dict()

        self._brokerage = brokerage
        self._data_source = data_source

        self._is_day_trader = is_day_trader

        self._active = False

        self._cash_value = cash_value

    @property
    def is_active(self):
        return self._active

    @is_active.setter
    def is_active(self, state: bool):
        self._active = state

    @property
    def cash_value(self):
        return self._cash_value

    @property
    def portfolio(self):
        """dict: Return stocks that the trader is managing"""

        return self._portfolio

    @property
    def brokerage(self):
        return self._brokerage

    @property
    def data_source(self):
        return self._data_source

    @property
    def has_any_stock(self):
        return True if len(self._portfolio) else False

    @property
    def day_trader(self):
        return self._is_day_trader

    @day_trader.setter
    def day_trader(self, is_a_day_trader: bool):
        self._is_day_trader = is_a_day_trader

    def has_stock(self, symbol):
        return symbol in self._portfolio.keys()

    def has_position(self, symbol: bool):
        return self._portfolio[symbol]['quantity'] > 0

    def is_trading(self, symbol: bool):
        return self.has_position(symbol=symbol)

    def add_stock(self, symbol: str, quantity=0, stock_value=0):
        if self.has_stock(symbol=symbol) is True:
            raise ValueError(f"'{symbol}' is already in portfolio")
        else:
            portfolio_record = {
                'ticker': symbol,
                'category': 'stock',
                'quantity': quantity,
                'cost': quantity * stock_value
            }
            self._portfolio[symbol] = portfolio_record

    # def add_stocks(self, symbol_list, quantity_list, stock_value_list):
    #     iteration_list = zip(symbol_list, quantity_list, stock_value_list)
    #
    #     for symbol, quantity, stock_value in iteration_list:
    #         self.add_stock(symbol=symbol, quantity=quantity,
    #                        stock_value=stock_value)

    def verify_stock_transaction(self, order_type, symbol, quantity,
                                 stock_value):

        if symbol not in self._portfolio.keys():
            raise ValueError('You cannot sell a stock that is not managed')

        if order_type == 'sell':

            if self._portfolio[symbol]['quantity'] <= 0:
                raise ValueError('You cannot sell a stock that has no quantity')
            elif self._portfolio[symbol]['quantity'] < quantity:
                raise ValueError('You cannot sell more than you own')

        elif order_type == 'buy':

            if (quantity * stock_value) > self._cash_value:
                raise ValueError("You cannot buy with value more than cash")

    def sold_stock(self, symbol: str, quantity, stock_value):
        self.verify_stock_transaction(order_type='sell', symbol=symbol,
                                      quantity=quantity,
                                      stock_value=stock_value)

        self._portfolio[symbol]['quantity'] -= quantity
        self._portfolio[symbol]['cost'] -= quantity * stock_value
        self._cash_value += quantity * stock_value

    def buy_stock(self, symbol: str, quantity, stock_value):
        self.verify_stock_transaction(order_type='buy', symbol=symbol,
                                      quantity=quantity,
                                      stock_value=stock_value)

        self._portfolio[symbol]['quantity'] += quantity
        self._portfolio[symbol]['cost'] += quantity * stock_value
        self._cash_value -= quantity * stock_value

    def portfolio_value(self):
        quan = [(i['ticker'], i['quantity']) for i in self.portfolio.values()]

        result = 0
        for ticker, quantity in quan:
            price = self.data_source.get_stock_latest(ticker)
            total_price = price['current'] * quantity
            result += total_price

        result += self.cash_value

        return result

    @abc.abstractmethod
    async def start(self):
        # @check_has_any_stocks
        # @execution_async_interval_loop
        pass


class RandomStockTrader(StockTrader):

    def __init__(self, brokerage: brokerage.Brokerage,
                 data_source: api.DataSource, freq=30, freq_type='static',
                 timezone: str = 'EST', is_day_trader=True, cash_value=10000):

        super().__init__(brokerage=brokerage, data_source=data_source,
                         timezone=timezone, is_day_trader=is_day_trader,
                         cash_value=cash_value)

        self._execution_freq = freq
        self._execution_freq_type = freq_type

        cash_to_add = cash_value - self._brokerage.account_cash

        self._brokerage.add_cash(cash_to_add)

    @property
    def execution_frequency(self):
        return self._execution_freq

    @property
    def execution_frequency_type(self):
        return self._execution_freq_type

    @check_has_any_stocks
    @interval_event_loop
    def start(self):

        n_stocks = random.randint(0, len(self.portfolio))
        ticker_list = random.sample(self.portfolio.keys(), n_stocks)
        buy_sell_list = random.choices([0, 1], k=len(ticker_list))

        trans_type = None
        for ticker, buy_sell in zip(ticker_list, buy_sell_list):
            if self.portfolio[ticker]['quantity'] > 0:
                if buy_sell == 1:  # buy some stock
                    quantity = random.randint(1, 2)

                    try:

                        trans = self.brokerage.order_buy_market(ticker, quantity)
                        self.buy_stock(ticker, trans['quantity'], trans['price'])
                        trans_type = 'Buy'

                    except ValueError:
                        continue

                else:  # sell some stock
                    quantity = random.randint(1, self.portfolio[ticker]['quantity'])

                    trans = self.brokerage.order_sell_market(ticker, quantity)
                    self.sold_stock(ticker, trans['quantity'], trans['price'])
                    trans_type = 'Sell'

            else:  # you have to buy
                quantity = random.randint(1, 2)

                try:

                    trans = self.brokerage.order_buy_market(ticker, quantity)
                    self.buy_stock(ticker, trans['quantity'], trans['price'])
                    trans_type = 'Buy'

                except ValueError:
                    continue

            pmsg = f"{trans_type} {trans['quantity']} of {ticker}. Value: " \
                   f"{round(self.portfolio_value(), 2)}"
            print(pmsg)




        # for ticker in ticker_list:
        #
        # if num_of_stocks_to_check:
        #     tickers = random.sample(self.stocks, num_of_stocks_to_check)
        #
        #     for ticker in tickers:
        #         should_buy_sell = random.randint(0, 1)
        #
        #         if should_buy_sell:
        #             if self.has_position(ticker): # sell it
        #                 num_owned = self.portfolio[ticker]['quantity']
        #                 random.randint(1, num_owned)
        #                 # self.brokerage.order_sell_market()
        #
        #             else:
        #                 pass
