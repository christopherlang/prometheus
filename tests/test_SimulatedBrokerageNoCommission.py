import pytest
from prometheus import brokerage
from prometheus.datasource import api


DSOURCE = api.SecuritySource()


def test_add_cash():
    broker = brokerage.SimulatedBrokerageNoCommission(DSOURCE)
    broker.add_cash(10000)

    assert broker.account_cash == 10000


def test_account_total_value_property():
    broker = brokerage.SimulatedBrokerageNoCommission(DSOURCE)
    broker.add_cash(10000)

    assert broker.account_total_value == 10000


def test_order_buy_market_values_affect():
    broker = brokerage.SimulatedBrokerageNoCommission(DSOURCE)
    broker.add_cash(10000)

    output = broker.order_buy_market('AAPL', 10, price_override=150)

    assert broker.account_equity == 10 * 150
    assert broker.account_cash == 10000 - (10 * 150)


def test_order_buy_market_raise_buying_power():
    broker = brokerage.SimulatedBrokerageNoCommission(DSOURCE)
    broker.add_cash(10000)

    with pytest.raises(ValueError):
        broker.order_buy_market('AAPL', 100, price_override=150)