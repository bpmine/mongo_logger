import pika
import requests
import json
from pymongo import MongoClient
import re
from datetime import datetime
from tinyot_parser import parse

pTopicWithAddr=re.compile(r'^(.+)\.([0-9])+$')
def extractAddr(key,base):
    m=pTopicWithAddr.match(key)
    if m!=None:
        topic=m.group(1)
        if (base==topic):
            addr=int(m.group(2))
            return addr

    return None
    

class RabbitGW:
    ip=None
    login=None
    passe=None

    conn=None
    ch=None

    def connect(self):
        credentials = pika.PlainCredentials(self.login, self.passe)

        self.conn = pika.BlockingConnection(
            pika.ConnectionParameters(self.ip,
                                      5672,
                                      '/',
                                      credentials)
            )
        self.ch = self.conn.channel()
        
    
    def __init__(self,ip,login,passe,port_mongo):
        self.ip=ip
        self.login=login
        self.passe=passe
        self.port_mongo=port_mongo

        self.connect()

        self.mg=MongoClient(host=['localhost:%d' % (self.port_mongo)])

    #def sendChat(self,msg):
    #    dta={
    #        "typ":"chat",
    #        "dst":"all",
    #        "msg":msg
    #         }
        
    #    self.send(json.dumps(dta))

    #def send(self,msg):
    #    self.ch.basic_publish(exchange="minetest",
    #                          routing_key=".minetest.server",
    #                          body=msg)

    def close(self):
        self.ch.close()
        self.conn.close()

    def cb_logs(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('Reception de LOG: %s -> %s' % (key,msg) )

        num=extractAddr(key,'.minou.log')
        if num!=None:
            doc={
                'date_comm':datetime.now(),
                'addr':num,
                'msg':msg
                }

            try:
                col=self.mg.minou.logs
                col.insert_one(doc)
                self.ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as ex:
                print('ERROR DB:' + str(ex))
        else:
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
 
        
    def cb_data(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('DATA: %s -> %s' % (key,msg) )

        num=extractAddr(key,'.minou.data')
        if num!=None:
            try:
                doc=json.loads(msg)
            except Exception as ex:
                print('ERROR JSON: '+str(ex))
                self.ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            doc['date_comm']=datetime.now()
            doc['addr']=num

            try:
                col=self.mg.minou.datas
                col.insert_one(doc)
                self.ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as ex:
                print('ERROR DB: '+str(ex))
                return
        else:
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
            

    def cb_maison(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('MAISON: %s -> %s' % (key,msg) )

        try:
            doc=json.loads(msg)
        except Exception as ex:
            print('ERROR JSON: '+str(ex))
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        doc['date_comm']=datetime.now()
        doc['key']=key

        try:
            col=self.mg.maison.events
            col.insert_one(doc)
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            print('ERROR DB: '+str(ex))
            return

    def cb_tinyot(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('TINYOT: %s -> %s' % (key,msg) )

        try:
            doc=parse(msg)
        except Exception as ex:
            print('ERROR JSON: '+str(ex))
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        doc['date_comm']=datetime.now()
        doc['key']=key

        try:
            col=self.mg.tinyot.datas
            col.insert_one(doc)
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            print('ERROR DB: '+str(ex))
            return

    def cb_oiso(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('OISO DATAS: %s -> %s' % (key,msg) )

        try:
            doc=json.loads(msg)
        except Exception as ex:
            print('ERROR JSON: '+str(ex))
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        doc['date_comm']=datetime.now()
        doc['key']=key

        try:
            col=self.mg.oiso.datas
            col.insert_one(doc)
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            print('ERROR DB: '+str(ex))
            return

    def cb_oiso_events(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('OISO EVENTS: %s -> %s' % (key,msg) )

        try:
            doc=json.loads(msg)
        except Exception as ex:
            print('ERROR JSON: '+str(ex))
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        doc['date_comm']=datetime.now()
        doc['key']=key

        try:
            col=self.mg.oiso.events
            col.insert_one(doc)
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            print('ERROR DB: '+str(ex))
            return

    def cb_oiso_logs(self,ch, method, properties, body):
        key=method.routing_key
        msg=body.decode()

        print('OISO LOGS: %s -> %s' % (key,msg) )

        try:
            doc={'msg':msg}
        except Exception as ex:
            print('ERROR JSON: '+str(ex))
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        doc['date_comm']=datetime.now()
        doc['key']=key

        try:
            col=self.mg.oiso.logs
            col.insert_one(doc)
            self.ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            print('ERROR DB: '+str(ex))
            return

    def start(self):
        self.ch.basic_consume(queue="minou.logs", on_message_callback=self.cb_logs)
        self.ch.basic_consume(queue="minou", on_message_callback=self.cb_data)
        self.ch.basic_consume(queue="maison", on_message_callback=self.cb_maison)
        self.ch.basic_consume(queue="tinyot_devices", on_message_callback=self.cb_tinyot)
        self.ch.basic_consume(queue="oiso", on_message_callback=self.cb_oiso)
        self.ch.basic_consume(queue="oiso.events", on_message_callback=self.cb_oiso_events)
        self.ch.basic_consume(queue="oiso.logs", on_message_callback=self.cb_oiso_logs)

        self.ch.start_consuming()


   
with open('credentials.txt','r') as json_file:
    data = json.load(json_file)

IP=data['ip']
LOGIN=data['login']
PASS=data['pass']

PORT_MONGO=data['port_mongo']

print('Start service logger...')

r=RabbitGW(IP,LOGIN,PASS,PORT_MONGO)
r.start()

        



