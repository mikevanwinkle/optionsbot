import string, requests, polygon, datetime, os
from numpy import poly
from operator import contains
from sqlalchemy import Float, func
from dotenv import load_dotenv
load_dotenv()

URLS = {
  "contracts": "https://api.polygon.io/v3/reference/options/contracts"
}

COLUMNS = {
  'c': 'close',
  'h': 'high',
  'l': 'low',
  't': 'date',
  'v': 'volume',
  'vw': 'vw',
  'o': 'open',
  'n': 'number'
}

def polygon_url(name):
  if "https://" in name:
    return f"{name}&apiKey={os.environ.get('apikey')}"
  return f"{URLS[name]}?apiKey={os.environ.get('apikey')}"

def import_options_tickers(symbol):
  from src.models.contracts import Contracts
  contracts = Contracts()
  ticker = symbol
  req = requests.get(polygon_url('contracts') + f"&underlying_ticker={ticker}")
  data = req.json()
  go = True
  while go:
    for result in req.json()['results']:
      if not contracts.search("ticker", result['ticker'], one_entry=True):
        print(f"Adding {result['ticker']} ... ")
        contracts.update({"ticker": result['ticker'], "underlying": ticker})
    req = requests.get(polygon_url(data['next_url']))
    data = req.json()
    if not data and not data['next_url']: go = False

def get_aggregate_bars(contract, from_date: datetime.date, to_date: datetime.date):
  client = polygon.OptionsClient(os.environ.get("apikey"))
  print(contract, from_date.strftime("%Y-%m-%d"))
  return client.get_aggregate_bars(contract, from_date=from_date.strftime("%Y-%m-%d"), to_date=to_date.strftime("%Y-%m-%d"))

def get_price_history(ticker, from_date: datetime.date, to_date: datetime.date):
  client = polygon.StocksClient(os.environ.get('apikey'))
  return client.get_aggregate_bars(ticker, from_date=from_date.strftime("%Y-%m-%d"), to_date=to_date.strftime("%Y-%m-%d"))


class Adjuster:
  def __init__(self, ticker: string) -> None:
    self.ticker = ticker
    pass

  def adjust(self, date, price) -> Float:
    return price

class TSLAAjuster(Adjuster):
  def adjust(self, date, price) -> Float:
    from datetime import datetime
    new_price = price
    split_1 = datetime.strptime("2022-08-24", "%Y-%m-%d")
    if date <= split_1:
      new_price = price / 3
    return new_price

def load_price_adjuster(ticker):
  adjusters = {
    "TSLA": TSLAAjuster
  }
  adj = adjusters[ticker](ticker)
  return adj
