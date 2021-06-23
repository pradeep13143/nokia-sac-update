from flask import Flask

application = Flask(__name__)

application.config.from_pyfile('config.py')

from routes import *

if __name__ == "__main__":
    application.run('0.0.0.0', 5000)
