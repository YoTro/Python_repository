
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
        filename = "./amapkey.json"
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                key = json.load(f)['key']
                f.close()    
        self.key = key
    def geocode(self, address):
        location = ""
        citycode = ""
        if(address != ""):
            url = "https://restapi.amap.com/v3/geocode/geo?address={}&output=json&key={}".format(address, self.key)
            try:
                response = requests.request("GET", url, timeout=15)
                location = response.json()['geocodes'][0]['location']
                citycode = response.json()['geocodes'][0]['citycode']
            except Exception as e:
                print(e)
        return location, citycode
    def getDuration(self, trip_mode, origin, destination, citycode):
        url = "https://restapi.amap.com/v5/direction/{}?key={}&origin={}&destination={}&show_fields=cost".format(trip_mode, self.key, origin, destination)
        if trip_mode == "walking":
            url += "isindoor=0"
        if trip_mode == "transit/integrated":
            url += "&city1={}&city2={}".format(citycode[0], citycode[1])
        duration = 99999999999999
        try:
            response = requests.request("GET", url, timeout=15)
            if (trip_mode == 'transit/integrated'):
                for item in response.json()['route']['transits']:
                    duration = int(item['cost']['duration']) if int(item['cost']['duration']) < duration else duration
            else:
                for item in response.json()['route']['paths']:
                    if 'cost' not in item:
                        duration = int(item['duration'])
                    else:
                        duration = int(item['cost']['duration']) if int(item['cost']['duration']) < duration else duration
        except Exception as e:
            print(e)
            duration = 0
        return duration
if __name__ == '__main__':
    amap = Amap()
    amap.getKey()
    origin = "广东省深圳市龙华区坳头老村9号"
    destination = ""
    filename = './zhipinjobs_民治.csv'
    df = pd.read_csv(filename)
    saved_path = './data.csv'
    destinations = df['Address'].fillna(0)
    data = []
    location_origin, city1 = amap.geocode(origin)
    for destination in tqdm(destinations):
        try:
            if amap.key !="":
                d = [destination]
                location_destination, city2 = amap.geocode(destination)
                citycode = [city1, city2]
                trip_modes = ['driving', 'bicycling', 'electrobike', 'walking', 'transit/integrated']
                for trip_mode in trip_modes:
                    duration = amap.getDuration(trip_mode, location_origin, location_destination, citycode)
                    delta = datetime.timedelta(seconds=duration)
                    d.append(str(delta))
                data.append(d)
            else:
                break
        except Exception as e:
            print(e)
    df = pd.DataFrame(data, columns=['Address']+trip_modes)
    df.to_csv(saved_path, index=False, encoding='utf-8-sig')