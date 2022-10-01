import re
import datetime;
import string
import json

##obj_exemple={
##    'imei':'AA867047043886038',
##    'latitude':4556.123,
##    'longitude':11.35,
##    ...
##    'num':9
##    }


def parse(msg):
    obj={}
    
    if msg[0:2]!='$$':
        raise Exception("Bad frame header %s" % (msg[0:2]))

    length=msg[2:6];
    if not length.isdigit():
        raise Exception("Bad frame length format %s" % (length))

    length=int(length)
    obj['length']=length;
    if len(msg)!=length:
        raise Exception("Frame length incorrect %d expected but %d received" % (length,len(msg)))

    cs=msg[-2:]
    if not all(c in string.hexdigits for c in cs):
        raise Exception("Bad cs format: %s" % (cs))
    cs=int(cs,16)

    ## Verifier ici le checksum

    
    
    pack=msg[6:8]
    obj['pack']=pack;
    if pack=='AA':        
        elms=msg[8:].split('|');
        if len(elms)!=2:
            raise Exception("imei separator not found into: %s" % (msg[8:]))
            
        obj['imei']=elms[0];
        datas=elms[1][:-2];
        obj['datas']=datas;

        #calc_cs = 0

        #for elm in bytes(msg[-2], "ascii"):
        #    calc_cs=calc_cs^elm
        #calc_cs = "%02x" % (calc_cs)
        #print(calc_cs)
        #print(cs)


        pat=re.compile('^([0-9]{8})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{4})([a-zA-Z0-9]{4})([a-zA-Z0-9]{4})([0-9]{2})([0-9]{2})([0-9]{3})([0-9]{3})([0-9\.]{4})([0-9]{7})([0-9]{2})([0-9\.]{7})([NS]{1})([0-9]{3})([0-9\.]{7})([EW]{1})([0-9]{4})$');
        m=pat.match(datas)
        if m!=None:
            obj['vehicule_status']=m.group(1)

            year=int(m.group(2))+2000
            month=int(m.group(3))
            day=int(m.group(4))
            hour=int(m.group(5))
            minute=int(m.group(6))
            second=int(m.group(7))
            #date=datetime.datetime(year,month,day,hour,minute,second)
            #obj['date']=date
            obj['date']='%04d-%02d-%02dT%02d:%02d:%02dZ' % (year, month, day, hour, minute, second)

            obj['batt']=int(m.group(8))
            obj['supply']=int(m.group(9))

            tmp=m.group(10)
            a=int(tmp[0:2])
            b=int(tmp[2:])
            f=float(b/100)+a
            obj['adc1']=f

            obj['lacci']=m.group(11)
            obj['cellid']=m.group(12)
            obj['sats']=int(m.group(13))
            obj['signal']=int(m.group(14))
            obj['angle']=int(m.group(15))
            obj['speed']=int(m.group(16))

            obj['hdop']=float(m.group(17))
            obj['mileage']=int(m.group(18))
            obj['latitude']=int(m.group(19)) + float(m.group(20))/60
            obj['ns']=m.group(21)
            obj['longitude']=int(m.group(22)) + float(m.group(23))/60
            obj['ew']=m.group(24)

            obj['serial']=m.group(25)

            return obj
        else:
            raise Exception('Bad frame format')
            
    else:
        raise Exception('Bad pack delimiter')

    return None


def test_parse():
    obj=parse('$$0108AA867047043886038|0000400021022601504940040400580A51AB070617100001.500000004536.9766N00514.4476E00142B')
    print(json.dumps(obj, indent=4))
    assert obj['length']==108,'Taille incorrecte'
    assert obj['latitude']==45.616276666666664
    assert obj['longitude']==5.240793333333333
    assert obj['ew']=="E"
    print("tests OK")

if __name__=='__main__':
    test_parse()

