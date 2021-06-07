from time import sleep
from json import dumps
import requests 
import pandas as pd 
import numpy as np
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
url_temp_externe = 'http://history.openweathermap.org/data/2.5/history/city?q=Paris,FR&units=metric&&type=hour&start=1593561600&end=1594166400&appid=5b522f453dd299386049b7ee5a78c177&units=metric'
r = requests.get(url = url_temp_externe)
results= r.json()
data = results["list"]
producer.send('BigdataTopic', value=data)
step = 604800
val_finale =1622592000
val_inter =1594166400
val = val_inter
while val_inter < val_finale:
    i=0
    while i<=3 : 
        val_inter = val_inter + step
        url_temp_externe_2 = 'http://history.openweathermap.org/data/2.5/history/city?q=Paris,FR&units=metric&&type=hour&start='+ str(val)+'&end='+ str(val_inter)+'&appid=5b522f453dd299386049b7ee5a78c177&units=metric'
        r1 = requests.get(url = url_temp_externe_2)
        results1= r1.json()
        data = results1["list"]
        i+=1
        producer.send('BigdataTopic', value=data)
        
    
    sleep(900) 





