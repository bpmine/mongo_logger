from flask import Flask
from flask import request
from flask import jsonify
from flask_httpauth  import HTTPBasicAuth

from pymongo import MongoClient
import datetime

import sys
import json
from json import JSONEncoder

def getClient():
    #return MongoClient(host=['127.0.0.1:27017'])
    return MongoClient(host=['127.0.0.1:2700'])

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def authenticate(username, password):
    if username and password:
        if username == 'bpc' and password == 'bpc':
            return True
        else:
            return False

    return False

ctr=0
@app.route('/test')
def test():
    global ctr
    ctr+=1
    return "[OK] - %d" % (ctr)

@app.route('/maison/sonnette/events', methods=['GET'])
@auth.login_required
def getSonnette():
    cln=getClient()

    res=cln.maison.events.find({},{'_id':False}).sort('date_comm',-1).limit(4)
    ret=[]
    for d in res:
        ret.append(d)
    
    cln.close()

    return json.dumps(ret,cls=DateTimeEncoder)
    
    
@app.route('/minou/status', methods=['GET'])
@auth.login_required
def getMinouStatus():
    cln=getClient()

    res=cln.minou.datas.find({'addr':1},{'_id':False}).sort('date_comm',-1).limit(1)
    for doc in res:
        return json.dumps(doc,cls=DateTimeEncoder)

    return {}


@app.route('/tinyot/devices/<imei>/status', methods=['GET'])
@auth.login_required
def getDeviceStatus(imei):
    cln=getClient()

    res=cln.tinyot.datas.find({'imei':imei}).sort('date',-1).limit(1)
    for doc in res:
        return json.dumps(doc,cls=DateTimeEncoder)

    return {}

@app.route('/tinyot/devices/<imei>/history', methods=['GET'])
@auth.login_required
def getDeviceHistory(imei):
    cln=getClient()

    res=cln.tinyot.datas.find({'imei':imei}).sort('date',-1)
    if res!=None:
        return json.dumps(list(res),cls=DateTimeEncoder)
    else:
        return json.dumps([])

@app.route('/tinyot/devices', methods=['GET'])
@auth.login_required
def getDevicesList():
    cln=getClient()

    res=cln.tinyot.datas.aggregate([{'$group':{'_id':'$imei'}}])
    if res!=None:
        return json.dumps(list(res))
    else:
        return json.dumps([])

app.run()
    


