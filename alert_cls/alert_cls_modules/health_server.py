from flask import Flask
from flask_wtf import CSRFProtect
import logging

app = Flask(__name__)
csrf = CSRFProtect(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/healthz', methods=['GET'])
def healthz():
     logger.debug("Health check endpoint called.")
     return 'OK', 200

if __name__ == '__main__':
     logger.debug("Starting health server on 0.0.0.0:8080.")
     app.run(host='0.0.0.0', port=8080)
