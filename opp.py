from asyncio import events
from distutils.log import debug
from flask import Flask, render_template, make_response, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS, cross_origin
import json

app = Flask(__name__)
socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")
CORS(app)

@cross_origin()
@app.route('/')
def index():
  return make_response(jsonify({'test': 1}), 200)

@socketio.on('sub:options')
def run_options(args):
  socketio.emit("options" , {"test": 1})

@cross_origin()
@socketio.on('connect')
def test_connect():
  socketio.emit('after connect', {'data':'Let us learn Web Socket in Flask'})

if __name__ == '__main__':
  socketio.run(app, debug=True, port=5999)