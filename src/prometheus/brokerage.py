import abc
import uuid
import robin_stocks as robin
from InvestopediaApi import ita
from prometheus import utils
from prometheus.datasource import api


class Brokerage(metaclass=abc.ABCMeta):

    def __init__(self, commission_type_buy=None, commission_type_sell=None,
                 commission_fee_buy=None, commission_fee_sell=None):

        self.set_commission('buy', commission_type_buy, commission_fee_buy)
        self.set_commission('sell', commission_type_sell, commission_fee_sell)

    @property
    def account_equity(self):
        return self.get_account_value()['equity']

    @property
    def account_extended_hours_equity(self):
        return self.get_account_value()['extended_hours_equity']

    @property
    def account_cash(self):
        return self.get_account_value()['cash']

    @property
    def account_dividend_total(self):
        return self.get_account_value()['dividend_total']

    @property
    def account_values(self):
        return self.get_account_value()

    @property
    def account_total_value(self):
        return sum([i for i in self.get_account_value().values()])

    @property
    def commission_types(self):
        comm_type = {
            'buy': self._commission_type_buy,
            'sell': self._commission_type_sell
        }

        return comm_type

    @property
    def commission_fees(self):
        comm_fee = {
            'buy': self._commission_fee_buy,
            'sell': self._commission_fee_sell
        }

        return comm_fee

    def set_commission(self, category, commission_type, commission_fee=None):

        if category == 'sell':
            self._commission_type_sell = commission_type
            self._commission_fee_sell = commission_fee
        elif category == 'buy':
            self._commission_type_buy = commission_type
            self._commission_fee_buy = commission_fee
        else:
            raise ValueError(f"'{category}' category is not valid")

    @abc.abstractmethod
    def order_buy_market(self, symbol, quantity, price_type='ask_price',
                         time_in_force='gtc', extend_hours=False, **kwargs):
        """Submit a market order for a security that is executed immediately

        A market order (ordered placed at the current best price) is placed for
        the specified stock. You cannot set the price, just quantity. And how
        much you pay for each stock will be at "the best price" at the time
        of actual execution.

        Parameters
        ----------
        symbol : str
            The stock ticker of the stock to purchase
        quantity : float, int
            The number of stocks to buy. Can be float if broker supports
            fractional stock purchases
        price_type : {'ask_price', 'bid_price', None}
        time_in_force : {'gtc', 'gfd', 'ioc', 'opg'}
            'gtc' = good until cancelled
            'gfd' = good for the day
            'ioc' = immediate or cancel
            'opg' = execute at opening
        extend_hours : bool
            Allow trading during extended hours
        kwargs
            Other parameters can that be altered for a buy market

        Returns
        -------
        """

        pass

    @abc.abstractmethod
    def order_sell_market(self, symbol, value, num_stocks=None):
        pass

    @abc.abstractmethod
    def get_account_value(self):
        # Below keys should exist for all from now on
        value['equity'] = None
        value['extended_hours_equity'] = None
        value['cash'] = None
        value['dividend_total'] = None


class SimulatedBrokerageNoCommission(Brokerage):

    def __init__(self, data_source: api.DataSource):

        super().__init__()

        self._data_source = data_source

        # This is used to fake total equity
        self._account_value = {
            'equity': 0,
            'extended_hours_equity': 0,
            'cash': 0,
            'dividend_total': 0
        }

    def add_cash(self, cash):
        self.account_values['cash'] += cash

    def order_buy_market(self, symbol, quantity, price_type='ask_price',
                         time_in_force='gtc', extend_hours=False,
                         price_override=None):
        """Submit a market order for a security that is executed immediately

        A market order (ordered placed at the current best price) is placed for
        the specified stock. You cannot set the price, just quantity. And how
        much you pay for each stock will be at "the best price" at the time
        of actual execution.

        Parameters
        ----------
        symbol : str
            The stock ticker of the stock to purchase
        quantity : float, int
            The number of stocks to buy. Can be float if broker supports
            fractional stock purchases. This is ignored
        price_type : Ignored
        time_in_force : Ignored
        extend_hours : Ignored
        price_override : float, int
            For testing purposes. This overrides stock price

        Returns
        -------
        dict
            Contains information regarding the purchase of stocks, such as:
            [Order ID, State of Order, Price, quantity]

        Raises
        ------
        ValueError
            If the total value of the purchases exceeds cash value
        """

        if price_override is None:
            quote = self._data_source.get_stock_latest(symbol=symbol)
            stock_price = quote['current']
        else:
            stock_price = price_override

        total_purchase_price = stock_price * quantity

        if total_purchase_price > self.account_cash:
            raise ValueError("Purchase exceeds buying power")

        self._account_value['cash'] -= total_purchase_price
        self._account_value['equity'] += total_purchase_price

        result = {
            'order_id': uuid.uuid1(),
            'order_type': 'market',
            'transaction_type': 'buy',
            'order_state': 'complete',
            'price': stock_price,
            'total': total_purchase_price,
            'quantity': quantity
        }

        return result

    def order_sell_market(self, symbol, quantity, price_type='ask_price',
                          time_in_force='gtc', extend_hours=False,
                          price_override=None):

        if price_override is None:
            quote = self._data_source.get_stock_latest(symbol=symbol)
            stock_price = quote['current']
        else:
            stock_price = price_override

        total_sell_price = stock_price * quantity

        self._account_value['cash'] += total_sell_price
        self._account_value['equity'] -= total_sell_price

        result = {
            'order_id': uuid.uuid1(),
            'order_type': 'market',
            'transaction_type': 'sell',
            'order_state': 'complete',
            'price': stock_price,
            'total': total_sell_price,
            'quantity': quantity
        }

        return result

    def get_account_value(self):
        return self._account_value
