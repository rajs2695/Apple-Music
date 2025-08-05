# heartbeat.py

from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running on Render!"

def run():
    app.run(host='0.0.0.0', port=10000)

def heartbeat():
    t = Thread(target=run)
    t.daemon = True
    t.start()
