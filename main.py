from prometheus import utils
from prometheus.datasource import api
from prometheus import traders
from prometheus import brokerage
import asyncio
import json

# stock_pool = utils.securities.movers()
# stock_pool = stock_pool[stock_pool['last_trade_price'] >= MINI_PRICE]
# stock_pool = stock_pool[stock_pool['growth'] > 0]
# stock_pool = stock_pool.sort_values('growth', axis=0, ascending=False)
#
# consideration_pool = stock_pool.iloc[:5,:]
#
# # Create our API data source
DSOURCE = api.SecuritySource()
SPIGOT = api.SpigotTrades()
ROBIN_SIM = brokerage.SimulatedBrokerageNoCommission(data_source=DSOURCE)

rando_trader = traders.RandomStockTrader(brokerage=ROBIN_SIM,
                                         data_source=DSOURCE)

rando_trader.add_stock('AMZN')
rando_trader.add_stock('AAPL')
rando_trader.add_stock('TSM')
rando_trader.add_stock('TXN')
rando_trader.add_stock('MSFT')

rando_trader.is_active = True


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    #
    # SPIGOT.add_ticker('AMZN')
    # SPIGOT.add_ticker('AAPL')
    #
    # output_filename1 = 'C:/Users/chlan/Desktop/finnhub_output_aapl.json'
    # output_filename2 = 'C:/Users/chlan/Desktop/finnhub_output_amzn.json'
    #
    # coroutines = asyncio.gather(
    #     SPIGOT.spigot_start(),
    #     consumer(output_filename1, SPIGOT.get_trade_queue("AAPL")),
    #     consumer(output_filename2, SPIGOT.get_trade_queue("AMZN"))
    # )
    #
    # event_loop.run_until_complete(coroutines)
    coroutines = asyncio.gather(rando_trader.start())
    event_loop.run_until_complete(coroutines)
