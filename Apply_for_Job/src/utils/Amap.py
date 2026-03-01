
import os
import json
import requests
import datetime
import pandas as pd
from tqdm import tqdm

class Amap():
    def __init__(self):
        self.key = ""
    def getKey(self):
        key = ""
        filename = "../../config/amapkey.json"
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                key = json.load(f)['key']
                f.close()    
        self.key = key