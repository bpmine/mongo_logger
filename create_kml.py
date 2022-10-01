import csv
import simplekml
from pymongo import MongoClient

#cln=MongoClient(host=["127.0.0.1:2700"])
cln=MongoClient(host=["127.0.0.1:27017"])

kml=simplekml.Kml()

res=cln.tinyot.datas.find({'imei':'867047043886038'}).sort('date',1)
for doc in res:
  if doc['sats']>2:
    ns=doc['ns']
    lat=doc['latitude']
    if ns=='S':
      lat=-lat
    
    ew=doc['ew']
    lon=doc['longitude']
    if ew=='W':
      lon=-lon
   
    kml.newpoint(name=doc['date'], coords=[(lon,lat)])

kml.save('output.kml')
