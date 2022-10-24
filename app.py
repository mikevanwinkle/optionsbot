#! python
from ast import arg
from datetime import timedelta
from sys import argv
from time import strftime
import asyncio
from pprint import pprint as print
import pandas as pd
from lib.polygon_util import polygon_url
from dotenv import load_dotenv
import os

# dbpass = 'QQd07fn0fn85'
# dbuser = 'optbotuser'
# dbdb = 'optionsbot'
# dbhost = 'dbs.politicseq.com'
load_dotenv()

TICKER = "TSLA"

class Bot:
  def contracts(self):
    from lib.polygon_util import import_options_tickers
    from lib.models.contracts import Contracts

    contracts = Contracts()
    for ticker in import_options_tickers(TICKER):
      if not contracts.search("ticker", ticker, one_entry=True):
        print(f"Adding ticker: {ticker}")
      t = contracts.update({"ticker": ticker, "underlying": "AAPL"})

  def agg(self):
    from lib.models.contracts import Contracts
    from lib.models.aggregrates import Aggregates
    from lib.polygon_util import get_aggregate_bars, COLUMNS
    import pandas as pd
    contracts = Contracts()
    agg = Aggregates()
    import datetime
    from lib.models.contracts import Contracts
    # to_date = (datetime.datetime.today() - datetime.timedelta(days=14))
    to_date = datetime.datetime.today()
    from_date = (to_date - datetime.timedelta(days=14))
    # print(f"{from_date.strftime('%D')} => {to_date.strftime('%D')}")
    # to_date = datetime.datetime.today()
    conts =  contracts.getSet(where=f'ticker LIKE "O:{TICKER}2%"', order='asc')
    put_call_ratio = {}
    prices = {}
    for c in conts:
      import re
      matches = re.match('O:(?P<ticker>[A-Z]{3,4})(?P<date>[0-9]+)(?P<type>[C|P])(?P<dollars>[0-9]{5})(?P<cents>[0-9]{3})', c['ticker'])
      data = get_aggregate_bars(c['ticker'], from_date=from_date, to_date=to_date)
      # print(matches.group('type'))
      print(c)
      if 'results' in data.keys():
        for result in data['results']:
          row = {
            'ticker': c['ticker'],
            'underlying': c['underlying']
          }
          # print(datetime.datetime.fromtimestamp(result['t'] / 1000).strftime('%Y-%m-%d'))
          result['t'] = datetime.datetime.fromtimestamp(result['t'] / 1000).strftime('%Y-%m-%d')
          for k in result.keys():
            row[COLUMNS[k]] = result[k]
          agg.update(row)

  def analyze(self):
    from lib.models.aggregrates import Aggregates
    import pandas as pd
    lookback = 14
    agg = Aggregates()
    # calls = agg.getSet(where=f'ticker LIKE "O:{TICKER}22%C%"', offset=0, limit=11000, order='asc', order_by='ticker')
    # puts = agg.getSet(where=f'ticker LIKE "O:{TICKER}22%P%"', offset=0, limit=11000, order='asc', order_by='ticker')
    calls = agg.getResult(f"SELECT date, sum(volume), avg(vw) as vw, avg(high - low) as spread, avg(close), sum(number) FROM aggregates where ticker LIKE 'O:{TICKER}22%C%' GROUP BY date ORDER by date asc")
    puts = agg.getResult(f"SELECT date, sum(volume), avg(vw) as vw, avg(high - low) as spread, avg(close), sum(number) FROM aggregates where ticker LIKE 'O:{TICKER}22%P%' GROUP BY date ORDER by date asc")
    df_calls = pd.DataFrame(calls)
    df_calls['type'] = 'call'
    df_puts = pd.DataFrame(puts)
    df_puts['type'] = 'put'
    df = pd.concat([df_calls, df_puts])
    # df = df.drop(columns=['open', 'close', 'high', 'low'])
    print(df_puts)
    lookbackperiod = df_puts.iloc[-lookback:]
    cumret = (lookbackperiod.vw.pct_change() + 1).cumprod() - 1
    cum_change = cumret[cumret.last_valid_index()]

    print(cumret)

if __name__ == '__main__':
  bot = Bot()
  getattr(bot, argv[1])()