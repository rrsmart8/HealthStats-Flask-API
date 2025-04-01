import os
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

import logging
from logging.handlers import RotatingFileHandler
import time
import os


if not os.path.exists('results'):
    os.mkdir('results')

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

# Logging setup
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'webserver.log')

log_handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=5)
log_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
formatter.converter = time.gmtime  # use UTC
log_handler.setFormatter(formatter)

# Attach the handler to the Flask logger
webserver.logger.addHandler(log_handler)
webserver.logger.setLevel(logging.INFO)


webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

from app import routes


