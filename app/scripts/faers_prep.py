import json
import glob
import time
import pandas as pd
from kafka import KafkaProducer

import os
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = "data/faers"

TOPIC_MAP = {
    "DEMO": "faers_demo",
    "DRUG": "faers_drug",
    "REAC": "faers_reac",
    "OUTC": "faers_outc",
}