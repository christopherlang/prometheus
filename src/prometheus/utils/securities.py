import datetime
import pytz
import pandas
import robin_stocks as robin


def movers(source='robinhood', top=20):

    if source == 'robinhood':
        return movers_robinhood_top20()
    else:
        raise ValueError('Provided source is not supported')


def movers_robinhood_top20():
    r = robin.markets.get_top_movers()

    for sec_data in r:
        sec_data['ask_price'] = float(sec_data['ask_price'])
        sec_data['ask_size'] = int(sec_data['ask_size'])
        sec_data['bid_price'] = float(sec_data['bid_price'])
        sec_data['bid_size'] = int(sec_data['bid_size'])
        sec_data['last_trade_price'] = float(sec_data['last_trade_price'])
        sec_data['previous_close'] = float(sec_data['previous_close'])

        sec_data['adjusted_previous_close'] = (
            float(sec_data['adjusted_previous_close'])
        )

        # This value doesn't exist during trading hours, returns None
        # You'll need to use 'last_trade_price'
        if sec_data['last_extended_hours_trade_price'] is not None:
            sec_data['last_extended_hours_trade_price'] = (
                float(sec_data['last_extended_hours_trade_price'])
            )

            sec_data['growth'] = (sec_data['last_extended_hours_trade_price'] /
                                  sec_data['adjusted_previous_close'] - 1)
        else:
            sec_data['growth'] = (sec_data['last_trade_price'] /
                                  sec_data['adjusted_previous_close'] - 1)

        sec_data['previous_close_date'] = (
            datetime.date.fromisoformat(sec_data['previous_close_date'])
        )

        sec_data['trading_halted'] = bool(sec_data['trading_halted'])
        sec_data['has_traded'] = bool(sec_data['has_traded'])

        sec_data['updated_at'] = sec_data['updated_at'].replace('Z', '')
        sec_data['updated_at'] = (
            datetime.datetime.fromisoformat(sec_data['updated_at'])
        )

        sec_data['updated_at'] = pytz.UTC.localize(sec_data['updated_at'],
                                                   is_dst=False)

    result = pandas.DataFrame.from_records(r)
    result = result.set_index('symbol').drop('instrument', axis=1)

    return result
