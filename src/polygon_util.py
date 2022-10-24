from numpy import poly
import requests
from operator import contains
import polygon
import datetime
API_KEY = 'DzfIoW0e36xu0R_B48NxMO4t856DF00V'
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
    return f"{name}&apiKey={API_KEY}"
  return f"{URLS[name]}?apiKey={API_KEY}"

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
  client = polygon.OptionsClient(API_KEY)
  return client.get_aggregate_bars(contract, from_date=from_date.strftime("%Y-%m-%d"), to_date=to_date.strftime("%Y-%m-%d"))

def get_price_history(ticker, from_date: datetime.date, to_date: datetime.date):
  client = polygon.StocksClient(API_KEY)
  return client.get_aggregate_bars(ticker, from_date=from_date.strftime("%Y-%m-%d"), to_date=to_date.strftime("%Y-%m-%d"))

