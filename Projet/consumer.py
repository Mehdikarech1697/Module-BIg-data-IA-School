from time import sleep
from json import dumps
import requests 
import pandas as pd 
import numpy as np
from kafka import KafkaConsumer
from json import loads
import numpy as np


consumer1 = KafkaConsumer(
    'BigdataTopic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='bigdata-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

i = 0 
for m in consumer1:
     
    t = m.value
    if (i==0):
        df = pd.DataFrame(t)
        df["temperature"] = df["main"].apply(lambda x : x["temp"])
        df["humidité"] = df["main"].apply(lambda x : x["humidity"])
        df["temp_min"] = df["main"].apply(lambda x : x["temp_min"])
        df["temp_max"] = df["main"].apply(lambda x : x["temp_max"])
        df["pressure"] = df["main"].apply(lambda x : x["pressure"])
        df["wind_speed"] = df["wind"].apply(lambda x : x["speed"])
        df["wind_deg"] = df["wind"].apply(lambda x : x["deg"])
        df['weather'] = df["weather"].apply(lambda x:x[0]['main'])
        df["temperature"] = df["temperature"].apply(lambda x:x-273)
        df["temp_max"] = df["temp_max"].apply(lambda x:x-273)
        df["temp_min"] = df["temp_min"].apply(lambda x:x-273)
        df= df.loc[:,["dt","temp_min","temp_max","temperature","humidité","pressure","wind_speed","wind_deg","weather"]]
    else :
         df1 = pd.DataFrame(t)
         df1["temperature"] = df1["main"].apply(lambda x : x["temp"])
         df1["humidité"] = df1["main"].apply(lambda x : x["humidity"])
         df1["temp_min"] = df1["main"].apply(lambda x : x["temp_min"])
         df1["temp_max"] = df1["main"].apply(lambda x : x["temp_max"])
         df1["pressure"] = df1["main"].apply(lambda x : x["pressure"])
         df1["wind_speed"] = df1["wind"].apply(lambda x : x["speed"])
         df1["wind_deg"] = df1["wind"].apply(lambda x : x["deg"])
         df1['weather'] = df1["weather"].apply(lambda x:x[0]['main'])
         df1["temperature"] = df1["temperature"].apply(lambda x:x-273)
         df1["temp_max"] = df1["temp_max"].apply(lambda x:x-273)
         df1["temp_min"] = df1["temp_min"].apply(lambda x:x-273)
         df1= df1 .loc[:,["dt","temp_min","temp_max","temperature","humidité","pressure","wind_speed","wind_deg","weather"]]
         df = df.append(df1)
    i+=1 
    if ((i+1)%4==0):
        sleep(900)
        df.to_csv("meteo.csv")