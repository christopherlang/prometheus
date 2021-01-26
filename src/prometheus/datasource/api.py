import abc
import datetime
import copy
import typing
import asyncio
import json
import pytz
import pandas
import finnhub
import yfinance
import websockets
from prometheus import CFG
from prometheus import utils


class DataSource(metaclass=abc.ABCMeta):
    """Parent class defining a standardized data source API

    Classes that inherit this metaclass will have standardized properties and
    methods useful to retrieve data and get information about the data source
    itself, such as the name, api keys (if applicable), request logs, etc.

    The primary way to interact with classes that inherit this metaclass is
    the abstract method `get_data`, with parameters as needed. This method
    should return `pandas.core.frame.DataFrame` whenever possible. Other
    structures are allowed where when needed however. The output data
    structure, types, and others must be explicitly described in the method's
    docstring

    In child class make sure you `super().__init__()` before the class
    instantiates its own properties

    Standard Naming for Retrieval
    -----------------------------
    Method names for retrieving resource should adhere to the following:
        - All lowercase whenever possible
        - Max four words, delimited by '_'
        - start with 'get_' (the word get is exclusive to this)
        - followed by series type e.g. stocks, FX, etc. 1-2 words only
        - optionally followed by 'data' or 'series', where applicable
        - followed by additional qualifiers e.g. eod, intraday, etc.

    Ex. 'get_stock_series', `get_cpi_series`, `get_fx_series`

    Avoid a general 'retrieve_series' method. We're just standarding properties

    Parameters
    ----------
    timezone : str
        The timezone used for returning datetime object. Strongly recommended
        to leave as 'UTC'

    Attributes
    ----------
    source_name
    valid_name
    access_key
    api_url
    access_log
    timezone
    req_object
    """

    def __init__(self, timezone='UTC'):
        self._source_name = 'Source Name'
        self._valid_name = 'SourceName'
        self._access_key = '<apikey>'
        self._api_url = 'https://apiurl.com/'
        self._access_log = {
            'total_requests': 0,
            'last_request': None
        }
        self._timezone = pytz.timezone(timezone)
        self._client = None

    @property
    def source_name(self):
        """str: The pretty name of the data source"""

        return self._source_name

    @property
    def valid_name(self):
        """str: Alphanumeric form and underscore of `source_name`"""

        return self._valid_name

    @property
    def access_key(self):
        """str or None: The API key used to access the web API"""

        return self._access_key

    @property
    def api_url(self):
        """str or None: The API URL for accessing the resource"""

        return self._api_url

    @property
    def access_log(self):
        """dict: A running log of request operations"""

        return self._access_log

    @property
    def timezone(self):
        return self._timezone

    @property
    def client(self):
        return self._client

    @abc.abstractmethod
    def get_stock_latest(self, symbol, **kwargs):
        pass

    @abc.abstractmethod
    def get_stock_previous(self, symbol, **kwargs):
        pass

    @abc.abstractmethod
    def get_stock_info(self, symbol, **kwargs):
        pass

    @abc.abstractmethod
    def get_stock_symbols(self, **kwargs):
        pass

    def _update_log(self):
        """Internal method to update the `access_log` property"""

        self._access_log['total_requests'] += 1
        self._access_log['last_request'] = (
            self._timezone.localize(datetime.datetime.utcnow())
        )


class SecuritySource(DataSource):
    def __init__(self, timezone='EST'):
        self._source_name = 'Mixed Source'
        self._valid_name = 'mixed_source'
        self._access_key = None
        self._api_url = None
        self._access_log = {
            'total_requests': 0,
            'last_request': None
        }
        self._timezone = pytz.timezone(timezone)

        # Authenticating for Finnhub.io
        finnhub_keys = CFG.keys('finnhub')
        if finnhub_keys['use_sandbox']:
            finnhub_key = finnhub_keys['key_sandbox']
        else:
            finnhub_key = finnhub_keys['key']

        self._finnhub_client = finnhub.Client(api_key=finnhub_key)

    def get_stock_latest(self, symbol):
        """Retrieve realtime stock quote

        Source: Finnhub.io

        The stock quote returned is realtime (unknown delay, if any), including
        current price, open, close, etc. The endpoint return's its own date and
        time but it is known what it represents. It is on a 24 time lag. For
        example, it might be 1/15/2021 during trading hours but it still
        states it is 1/14/2021. This changes on 7:00PM ET, finally incrementing.

        This is pulled from Finnhub's `quote` endpoint, which comes with it's
        own date and time data. This is returned under the `datetime_endpoint`,
        while the query time is stored under `datetime_query`

        Parameters
        ----------
        symbol : str
            Stock ticker for which you want the latest prices

        Returns
        -------
        dict
            A record containing the following keys:
                ticker : str
                    Stock's ticker symbol
                datetime_endpoint : datetime.datetime
                    The date and time returned by the API
                datetime_query : datetime.datetime
                    The date and time for when this method was called
                open : float
                    Open price of the day
                current : float
                    Current price
                high : float
                    High price of the day
                low : float
                    Low price of the day
                close_previous : float
                    Previous close price
        """

        rec = self._finnhub_client.quote(symbol=symbol)

        rec['current'] = rec.pop('c')
        rec['high'] = rec.pop('h')
        rec['low'] = rec.pop('l')
        rec['open'] = rec.pop('o')
        rec['close_previous'] = rec.pop('pc')
        rec['datetime_endpoint'] = (
            datetime.datetime.utcfromtimestamp(rec.pop('t'))
        )
        rec['datetime_endpoint'] = pytz.UTC.localize(rec['datetime_endpoint'])
        rec['datetime_query'] = datetime.datetime.utcnow()
        rec['datetime_query'] = pytz.UTC.localize(rec['datetime_query'])
        rec['ticker'] = symbol

        rec['datetime_endpoint'] = (
            rec['datetime_endpoint'].astimezone(self._timezone)
        )
        rec['datetime_query'] = (
            rec['datetime_query'].astimezone(self._timezone)
        )

        key_order = ['ticker', 'datetime_endpoint', 'datetime_query', 'open',
                     'current', 'high', 'low', 'close_previous']

        result = {key: rec[key] for key in key_order}

        return result

    def get_stock_today(self, symbol):
        today = datetime.datetime.utcnow()
        today = pytz.UTC.localize(today)
        today = today.astimezone(pytz.timezone('EST'))
        tomorrow = today + datetime.timedelta(days=1)

        result = self.get_stock_historical(symbol=symbol, start=today,
                                           end=tomorrow, interval='1d')
        result = result.reset_index().to_dict('records')[0]
        result['date'] = result['date'].date()
        result['ticker'] = symbol

        key_order = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume',
                     'dividends', 'stock_splits']

        result = {key: result[key] for key in key_order}

        return result

    def get_stock_previous(self, symbol):
        """Retrieve last trading day prices for a stock

        Source: Yahoo! Finance

        Data returned is for the last trading day. The `date` is EST, though is
        also valid for UTC.

        Parameters
        ----------
        symbol : str
            Stock ticker

        Returns
        -------
        dict
            A record containing the following keys:
                ticker : str
                date : datetime.date
                open : float
                high : float
                low : float
                close : float
                volume : int
                dividends : int, float
                stock_splits : int, float
        """

        stock_data = yfinance.Ticker(ticker=symbol).history(period='2d')

        yesterday = utils.general.yesterday_utc()
        yesterday = pandas.to_datetime(yesterday.date())

        yesterday_stock_data = stock_data.loc[stock_data.index == yesterday, :]
        yesterday_stock_data = yesterday_stock_data.reset_index()

        result = yesterday_stock_data.to_dict('records')[0]
        result = {k.lower(): v for k, v in result.items()}
        result['date'] = result['date'].date()
        result['stock_splits'] = result.pop('stock splits')
        result['ticker'] = symbol

        key_order = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume',
                     'dividends', 'stock_splits']

        result = {key: result[key] for key in key_order}

        return result

    def get_stock_historical(self, symbol, start, end, interval='1d', **kwargs):
        """Retrieve historical prices for a stock

        Source: Yahoo! Finance

        The `interval` parameter determines the time serie's resolution. For
        example '1d' is for EOD prices for each day, '2d' is EOD prices for
        every 2 days, '1m' is for 1-minute resolution data, etc.

        Parameters
        ----------
        symbol : str
            Stock ticker
        start : str, datetime
            The start date and time for historical querying, inclusive
        end : str, datetime
            The end date and time for historical querying, exclusive
        interval : {1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo}
            As a string, sets the resolution of the time series returned
        kwargs
            Othey parameters to be send to `pyfinance.Ticker.history` method

        Returns
        -------
        pandas.DataFrame
            A data frame, indexed by the date/datetime, with the columns:
                ticker : str
                open : float
                high : float
                low : float
                close : float
                volume : float, int
                dividends : float, int
                stock_splits : float, int
        """

        stock_data = yfinance.Ticker(ticker=symbol)
        stock_data = stock_data.history(interval=interval, start=start, end=end,
                                        **kwargs)

        if stock_data.index.tzinfo:
            stock_data.index = stock_data.index.tz_convert(tz=self._timezone)

        # rename columns to lowercase and underscore instead of spaces
        stock_data.columns = [i.lower().replace(' ', '_')
                              for i in stock_data.columns]
        stock_data.index.names = [i.lower() for i in stock_data.index.names]
        stock_data['ticker'] = symbol

        col_order = ['ticker', 'open', 'high', 'low', 'close', 'volume',
                     'dividends', 'stock_splits']

        stock_data = stock_data[col_order]

        return stock_data

    def get_stock_eod(self, symbol, start, end, **kwargs):
        """Retrieve historical end-of-day (EOD) prices for a stock

        Source: Yahoo! Finance

        This is equivalent to method `self.get_stock_historical` with `interval`
        set to '1d' automatically

        Parameters
        ----------
        symbol : str
            Stock ticker
        start : str, datetime
            The start date and time for historical querying
        end : str, datetime
            The end date and time for historical querying
        kwargs
            Othey parameters to be send to `pyfinance.Ticker.history` method

        Returns
        -------
        pandas.DataFrame
            A data frame, indexed by the date, with the columns:
                open : float
                high : float
                low : float
                close : float
                volume : float, int
                dividends : float, int
                stock_splits : float, int
        """

        return self.get_stock_historical(symbol=symbol, start=start, end=end,
                                         interval='1d', **kwargs)

    def get_stock_intraday(self, symbol, start, end, **kwargs):
        """Retrieve historical 1-minute intraday prices for a stock

        Source: Yahoo! Finance

        This is equivalent to method `self.get_stock_historical` with `interval`
        set to '1m' automatically

        Parameters
        ----------
        symbol : str
            Stock ticker
        start : str, datetime
            The start date and time for historical querying
        end : str, datetime
            The end date and time for historical querying
        kwargs
            Othey parameters to be send to `pyfinance.Ticker.history` method

        Returns
        -------
        pandas.DataFrame
            A data frame, indexed by the datetime, with the columns:
                open : float
                high : float
                low : float
                close : float
                volume : float, int
                dividends : float, int
                stock_splits : float, int
        """

        return self.get_stock_historical(symbol=symbol, start=start, end=end,
                                         interval='1m', **kwargs)

    def get_stock_info(self, symbol, **kwargs):
        pass

    def get_stock_symbols(self, **kwargs):
        pass


class SpigotTrades:
    """Realtime access to Finnhub's trades WebSocket

    Provides access to realtime trades through Finnhub's trades WebSocket.

    This class is asynchronous program. Access to the stream is done via queues.
    Check out the method `get_trade_queue`, a `dict` that maintains a queue
    stream for each of the ticker it is tracking.

    Examples
    --------
    >>> SPIGOT = api.SpigotTrades()

    Adding some stock tickers to tracok

    >>> SPIGOT.add_ticker('AMZN')
    >>> SPIGOT.add_ticker('AAPL')
    >>> SPIGOT.add_ticker('TSM')

    You can also verify if a particular ticker is being tracked

    >>> SPIGOT.is_ticker_tracked('AMZN')
    True

    To get access to the streaming data, SpigotTrade stores the streamed results
    in a asyncio.Queue object, one for each ticker

    >>> SPIGOT.get_trade_queue('AMZN')

    SpigotTrades is a asyncio coroutine. To start the stream, use start method

    >>> asyncio.run(SPIGOT.spigot_start())

    Usually however, SpigotTrade (a producer) is one of many coroutines that
    needs to be run. Use asyncio.gather in those cases

    >>> async def consumer_print(stream_queue):
    ...     stream_record = await stream_queue.get()
    ...     print(stream_record)

    >>> stream_queue = SPIGOT.get_trade_queue('AMZN')
    >>> async_consumer = consumer(stream_queue)

    >>> async_coroutines = asyncio.gather(SPIGOT.spigot_start(), stream_queue)

    >>> event_loop = asyncio.get_event_loop()
    >>> event_loop.run_until_complete(async_coroutines)

    Parameters
    ----------
    max_tickers : int, optional
        Specify the maximum amount of tickers the class can track
    max_queue_size : int, optional
        Specify the maximum amount of stream records each `async.Queue` can
        store. Note: there is one queue object per ticker tracked

    Attributes
    ----------
    uri : str
        Returns the finnhub URI
    tickers : set[str]
        Returns the tickers currently being tracked
    ticker_size : int
        The maximum number of tickers that can be tracked
    queue_size : int
        The maximum number of stream records stored per queue object
    """

    def __init__(self, max_tickers=10, max_queue_size=10000):
        self._tickers_tracked = dict()
        self._max_tickers = max_tickers
        self._max_queue_size = max_queue_size

        # Authenticating for Finnhub.io
        finnhub_keys = CFG.keys('finnhub')
        if finnhub_keys['use_sandbox']:
            finnhub_key = finnhub_keys['key_sandbox']
        else:
            finnhub_key = finnhub_keys['key']

        self._uri = f"wss://ws.finnhub.io?token={finnhub_key}"

        self._added_tickers_line = set()
        self._removed_tickers_line = set()

    @property
    def uri(self):
        """The Uniform Resource Identifer (URI) of Finnhub"""
        return self._uri

    @property
    def tickers(self):
        """The collection of tickers that is currently being tracked"""
        return set(self._tickers_tracked.keys())

    @property
    def ticker_size(self):
        """The maximum number of tickers the object can track"""
        return self._max_tickers

    @property
    def queue_size(self):
        """The maximum number of trade records that can be store in a queue"""
        return self._max_queue_size

    def _has_tickers_to_add(self):
        return len(self._added_tickers_line) > 0

    def _has_tickers_to_remove(self):
        return len(self._removed_tickers_line) > 0

    def get_trade_queue(self, symbol):
        """Retrieves the queue object for a tracked ticker

        Parameters
        ----------
        symbol : str
            Company's ticker

        Returns
        -------
        asyncio.Queue
        """

        return self._tickers_tracked[symbol]

    def is_ticker_tracked(self, symbol):
        """Check if a company is already being tracked in the feed

        Parameters
        ----------
        symbol : str
            Company's ticker

        Returns
        -------
        bool
        """

        return symbol in self._tickers_tracked.keys()

    def add_ticker(self, symbol: str):
        """Add a realtime ticker feed

        Each ticker added will be subscribed to. Finnhub's feed will then
        start to include it in the WebSocket connection this object establishes.

        Each ticker is added to `self._tickers_tracked` object. Each ticker will
        have an `asyncio.Queue` FIFO queue object that maintains a running queue
        that contains the raw trade prices for that ticker.

        Parameters
        ----------
        symbol : str
            Company's ticker
        """

        if len(self._tickers_tracked) >= self._max_tickers:
            raise ValueError('Too many tickers')

        if self.is_ticker_tracked(symbol):
            raise ValueError(f"'{symbol}' is already tracked")

        else:
            # If this is new ticker, create new queue for tracking
            # AND add to tickers list which will signal when to disconnect
            # websocket and restart the connection
            self._tickers_tracked[symbol] = asyncio.Queue(self._max_queue_size)
            self._added_tickers_line.add(symbol)

    def add_tickers(self, symbols: typing.Collection[str]):
        """Add a sequence of tickers

        Convenience function for `self.add_ticker` method. Enables you to add
        a collection of tickers, which internally is just a loop.

        Parameters
        ----------
        symbols : collection of str
            A collection (e.g. list, tuple) of company tickers
        """

        for symbol in symbols:
            self.add_ticker(symbol=symbol)

    async def _subscribe_tickers(self, ws):
        """Subscribes to new tickers

        Will subscribe to tickers within the `self._tickers_tracked` dictionary
        (the keys specifically) by sending a JSON object to Finnhub.

        This should be called prior to any async loops. If there were no
        subscription prior ro async loops the loop will effectively not work.

        Parameters
        ----------
        ws : websockets.client.WebSocketClientProtocol
        """

        for symbol in self._tickers_tracked:
            subscribe_string = {'type': 'subscribe', 'symbol': symbol}
            subscribe_string = json.dumps(subscribe_string)

            await ws.send(subscribe_string)
            self._added_tickers_line.remove(symbol)

    async def spigot_start(self):
        """Coroutine to connect to Finnhub's realtime stock feed

        Requires that at least one ticker has been added to the object. Adding
        tickers allows the spigot to subscribe to them. As the data is fed into
        this object, it is distributed to different queues, one for each ticker.

        Each addition and removal of tickers to the object will cause the
        Websocket connection to disconnect, in order to subscribe to the new
        set of tickers.
        """

        if len(self._tickers_tracked) == 0:
            raise asyncio.QueueEmpty("There are no tickers to track.")

        # while loop here enables ability to add and subtract tickers that are
        # being tracked. Essentially, whenever you add/remove tickers for
        # tracking, you need to RESTART the websocket by disconnect, reconnect
        # then subscribe to those tickers
        while True:
            async with websockets.connect(self._uri) as ws:
                await self._subscribe_tickers(ws)

                async for spigot_data in ws:

                    spigot_data_parsed = json.loads(spigot_data)
                    trade_type = spigot_data_parsed['type']

                    try:
                        trade_records = spigot_data_parsed['data']
                    except KeyError:
                        print(spigot_data_parsed)

                    for trade_rec in trade_records:
                        symbol = trade_rec['s']

                        parsed_rec = _parse_finnhub_trade(trade_rec)
                        sec_queue = self._tickers_tracked[symbol]

                        while True:
                            try:
                                sec_queue.put_nowait(parsed_rec)
                            except asyncio.QueueFull:
                                # This assumes a FIFO queue
                                for _ in range(sec_queue.qsize()):
                                    sec_queue.get_nowait()
                                    sec_queue.task_done()

                    if (len(self._tickers_tracked) == 0 or
                           self._has_tickers_to_add() or
                           self._has_tickers_to_remove()):
                        break

            await self._trade_queue.put(None)


def _parse_finnhub_trade(trade_record):
    trade_record = copy.deepcopy(trade_record)

    trade_record['trade_condition'] = trade_record.pop('c')
    trade_record['last_price'] = trade_record.pop('p')
    trade_record['symbol'] = trade_record.pop('s')
    trade_record['datetime'] = trade_record.pop('t')
    trade_record['volume'] = trade_record.pop('v')

    trade_record['datetime'] = datetime.datetime.utcfromtimestamp(
        trade_record['datetime'] / 1000.0
    )

    trade_record['datetime'] = pytz.UTC.localize(trade_record['datetime'])

    return trade_record
