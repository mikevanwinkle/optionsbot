# bin/python
import websockets, websocket, asyncio
import _thread
import time
import rel
import json
import datetime
import re
import logging
import os
logger = logging.getLogger('websockets')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

CONNECTIONS = set
CNX_AUTH = None

def on_message(ws, messages):
    messages = json.loads(messages)
    for msg in messages:
      if "sym" in msg.keys() and "AAPL" in msg["sym"]:
        date = datetime.datetime.fromtimestamp(msg['s'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        matches = re.match('O:(?P<ticker>[A-Z]{3,4})(?P<date>[0-9]+)(?P<type>[C|P])(?P<dollars>[0-9]{5})(?P<cents>[0-9]{3})', msg["sym"])
        if not matches: print(msg['sym'])
        msg = {
          "ticker": matches.group('ticker'),
          "date": matches.group('date'),
          "type": matches.group('type'),
          "price": matches.group('dollars') + '.' + matches.group('cents'),
          "volume" : msg['v']

        }
        print(msg)

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

async def on_open(ws: websocket):
    print("Opened connection ")
    print(ws)
    CONNECTIONS.add(ws)
    try:
        await websocket.wait_closed()
    finally:
        CONNECTIONS.remove(websocket)
    ws.send(json.dumps({"action":"auth","params": os.getenv("API_KEY")}))
    ws.send(json.dumps({"action":"subscribe", "params":"A.*"}))

def listen():
  websocket.enableTrace(True)
  ws = websocket.WebSocketApp("wss://delayed.polygon.io/options",
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

  ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
  rel.signal(2, rel.abort)  # Keyboard Interrupt
  rel.dispatch()

async def do_auth(ws):
  await ws.send(json.dumps({"action":"auth","params": os.getenv("API_KEY")}))

async def async_listen():
  async with websockets.connect('wss://delayed.polygon.io/options') as ws:
    await ws.send(json.dumps({"action":"auth","params": os.getenv("API_KEY")}))
    await ws.send(json.dumps({"action":"subscribe", "params":"A.*"}))
    while True:
      try:
        messages = await ws.recv()
        on_message(ws, messages)

      except websockets.exceptions.ConnectionClosed:
        print('ConnectionClosed')
        is_alive = False
        break


def main():
  listen()

if __name__ == "__main__":
  asyncio.run(async_listen())


