import smtplib
import time 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#from http.client import IncompleteRead
import seaborn as sns
#%matplotlib inline
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import requests
import csv
import json
import sys
import pandas as pd

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

#import http.client
#http.client.HTTPConnection._http_vsn = 10
#http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'



# usar la fecha de ayer

list_of_arguments = sys.argv
print(len(sys.argv))

print(list_of_arguments[0]) 


def listToString(s): 
    
    # initialize an empty string
    str1 = "" 
    
    # traverse in the string  
    for ele in s: 
        str1 += ele  
    
    # return string  
    return str1 

incremental=False

if (len(sys.argv) == 2):
    if ( list_of_arguments[0] == 'S' ):
        incremental=True


now = datetime.now()

ayer=datetime.today() - timedelta(days=1)

str_fecha_hoy = now.strftime("%Y-%m-%d")

str_fecha_ayer = ayer.strftime("%Y-%m-%d")


url = 'https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/NCO/NCORetornaRegistros.cpe'
url_relativa = 'https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/'
patronReemplazo="../"
headers = {
'Accept': 'application/json, text/javascript, */*; q=0.01',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
'Connection': 'keep-alive',
'Content-Length': '1048',
'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' ,   
'Host': 'www.compraspublicas.gob.ec',
'Origin': 'https://www.compraspublicas.gob.ec',
'Referer': 'https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/NCO/FrmNCOListado.cpe',
'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
'sec-ch-ua-mobile': '?0',
'sec-ch-ua-platform': 'Windows',
'Sec-Fetch-Dest': 'empty',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Site': 'same-origin',
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
'Cookie': 'WRTCorrelator=000186140005f5c496443fd100003750; mySESSIONID=tdkCigkoBZKG8wIDoH5P3gk9g15; _ga=GA1.1.1650713493.1675371194; _ga_Q63K19FY90=GS1.1.1677599567.11.0.1677599570.0.0.0; NSC_IUUQT_wTfswfs_TPDF_Ofefufm=ffffffffc3a0662545525d5f4f58455e445a4a423660',
'X-Requested-With': 'XMLHttpRequest'}

params={'lot':1}

data={ 
'sEcho':1,
'iColumns':10,
'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
'iDisplayStart':0,
'iDisplayLength':0,
'mDataProp_0':'tipo_necesidad',
'sSearch_0':54092,
'bRegex_0':'false',
'bSearchable_0':'true',
'bSortable_0':'false',
'mDataProp_1':'codigo_contratacion',
'sSearch_1':'', 
'bRegex_1':'false',
'bSearchable_1':'true',
'bSortable_1':'true',
'mDataProp_2':'fecha_publicacion',
'sSearch_2':'',
'bRegex_2':'false',
'bSearchable_2':'true',
'bSortable_2':'false',
'mDataProp_3':'provincia',
'sSearch_3':'', 
'bRegex_3':'false',
'bSearchable_3':'true',
'bSortable_3':'false',
'mDataProp_4':'objeto_contratacion',
'sSearch_4':'',
'bRegex_4':'false',
'bSearchable_4':'true',
'bSortable_4':'false',
'mDataProp_5':'estado',
'sSearch_5':476,
'bRegex_5':'false',
'bSearchable_5':'true',
'bSortable_5':'true',
'mDataProp_6':'fecha_limite_propuesta',
'sSearch_6':'',
'bRegex_6':'false',
'bSearchable_6':'true',
'bSortable_6':'false',
'mDataProp_7':'url',
'sSearch_7':'',
'bRegex_7':'false',
'bSearchable_7':'true',
'bSortable_7':'false',
'mDataProp_8':'direccion_entrega',
'sSearch_8':'',
'bRegex_8':'false',
'bSearchable_8':'true',
'bSortable_8':'false',
'mDataProp_9':'contacto',
'sSearch_9':'',
'bRegex_9':'false',
'bSearchable_9':'true',
'bSortable_9':'true',
'sSearch':'',
'bRegex':'false',
'iSortCol_0':1,
'sSortDir_0':'desc',
'iSortingCols':1
}

page = requests.post(url,headers=headers,params=params, data=data)
dataWeb = page.json()


tope = int(dataWeb['iTotalDisplayRecords'])

print(tope)


### primer quiebre
numregistros=0
incremento=20000
while numregistros <= tope:
    # ir sacando cada 100
    if numregistros==0:
            data={ 
            'sEcho':0,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54092,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':476,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb=pd.DataFrame.from_records(dataWeb['data'])
            print(tope,numregistros,incremento)
    else:
            data={ 
            'sEcho':1,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros+1,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54092,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':476,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )
            print(tope,numregistros,incremento)
        
    numregistros=numregistros+incremento



print("Conteo de dfDataWeb:")    
len(dfDataWeb.index)
print(dfDataWeb.head(10))

data={ 
'sEcho':1,
'iColumns':10,
'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
'iDisplayStart':0,
'iDisplayLength':0,
'mDataProp_0':'tipo_necesidad',
'sSearch_0':54092,
'bRegex_0':'false',
'bSearchable_0':'true',
'bSortable_0':'false',
'mDataProp_1':'codigo_contratacion',
'sSearch_1':'', 
'bRegex_1':'false',
'bSearchable_1':'true',
'bSortable_1':'true',
'mDataProp_2':'fecha_publicacion',
'sSearch_2':'',
'bRegex_2':'false',
'bSearchable_2':'true',
'bSortable_2':'false',
'mDataProp_3':'provincia',
'sSearch_3':'', 
'bRegex_3':'false',
'bSearchable_3':'true',
'bSortable_3':'false',
'mDataProp_4':'objeto_contratacion',
'sSearch_4':'',
'bRegex_4':'false',
'bSearchable_4':'true',
'bSortable_4':'false',
'mDataProp_5':'estado',
'sSearch_5':384,
'bRegex_5':'false',
'bSearchable_5':'true',
'bSortable_5':'true',
'mDataProp_6':'fecha_limite_propuesta',
'sSearch_6':'',
'bRegex_6':'false',
'bSearchable_6':'true',
'bSortable_6':'false',
'mDataProp_7':'url',
'sSearch_7':'',
'bRegex_7':'false',
'bSearchable_7':'true',
'bSortable_7':'false',
'mDataProp_8':'direccion_entrega',
'sSearch_8':'',
'bRegex_8':'false',
'bSearchable_8':'true',
'bSortable_8':'false',
'mDataProp_9':'contacto',
'sSearch_9':'',
'bRegex_9':'false',
'bSearchable_9':'true',
'bSortable_9':'true',
'sSearch':'',
'bRegex':'false',
'iSortCol_0':1,
'sSortDir_0':'desc',
'iSortingCols':1
}
page = requests.post(url,headers=headers,params=params, data=data)
dataWeb = page.json()
tope = int(dataWeb['iTotalDisplayRecords'])

numregistros=0
incremento=2000
while numregistros <= tope:
    # ir sacando cada 100
    if numregistros==0:
            data={ 
            'sEcho':0,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54092,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':384,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )
            print(tope,numregistros,incremento)

    else:
            data={ 
            'sEcho':1,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros+1,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54092,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':384,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )
            print(tope,numregistros,incremento)
    numregistros=numregistros+incremento

# Revision Contrataciones

data={ 
'sEcho':1,
'iColumns':10,
'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
'iDisplayStart':0,
'iDisplayLength':0,
'mDataProp_0':'tipo_necesidad',
'sSearch_0':54089,
'bRegex_0':'false',
'bSearchable_0':'true',
'bSortable_0':'false',
'mDataProp_1':'codigo_contratacion',
'sSearch_1':'', 
'bRegex_1':'false',
'bSearchable_1':'true',
'bSortable_1':'true',
'mDataProp_2':'fecha_publicacion',
'sSearch_2':'',
'bRegex_2':'false',
'bSearchable_2':'true',
'bSortable_2':'false',
'mDataProp_3':'provincia',
'sSearch_3':'', 
'bRegex_3':'false',
'bSearchable_3':'true',
'bSortable_3':'false',
'mDataProp_4':'objeto_contratacion',
'sSearch_4':'',
'bRegex_4':'false',
'bSearchable_4':'true',
'bSortable_4':'false',
'mDataProp_5':'estado',
'sSearch_5':384,
'bRegex_5':'false',
'bSearchable_5':'true',
'bSortable_5':'true',
'mDataProp_6':'fecha_limite_propuesta',
'sSearch_6':'',
'bRegex_6':'false',
'bSearchable_6':'true',
'bSortable_6':'false',
'mDataProp_7':'url',
'sSearch_7':'',
'bRegex_7':'false',
'bSearchable_7':'true',
'bSortable_7':'false',
'mDataProp_8':'direccion_entrega',
'sSearch_8':'',
'bRegex_8':'false',
'bSearchable_8':'true',
'bSortable_8':'false',
'mDataProp_9':'contacto',
'sSearch_9':'',
'bRegex_9':'false',
'bSearchable_9':'true',
'bSortable_9':'true',
'sSearch':'',
'bRegex':'false',
'iSortCol_0':1,
'sSortDir_0':'desc',
'iSortingCols':1
}

page = requests.post(url,headers=headers,params=params, data=data)
dataWeb = page.json()


tope = int(dataWeb['iTotalDisplayRecords'])
tope

numregistros=0
incremento=2000
while numregistros <= tope:
    # ir sacando cada 100
    if numregistros==0:
            data={ 
            'sEcho':0,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54089,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':384,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )
            print(tope,numregistros,incremento)
    else:
            data={ 
            'sEcho':1,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros+1,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54089,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':384,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )        
            print(tope,numregistros,incremento)
    numregistros=numregistros+incremento
    
dfDataWeb.count()

dfDataWeb=dfDataWeb.replace(np.nan,'',regex=True)

dfDataWeb[dfDataWeb.estado!='En Curso']

# Revision Finalizadas

data={ 
'sEcho':1,
'iColumns':10,
'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
'iDisplayStart':0,
'iDisplayLength':0,
'mDataProp_0':'tipo_necesidad',
'sSearch_0':54089,
'bRegex_0':'false',
'bSearchable_0':'true',
'bSortable_0':'false',
'mDataProp_1':'codigo_contratacion',
'sSearch_1':'', 
'bRegex_1':'false',
'bSearchable_1':'true',
'bSortable_1':'true',
'mDataProp_2':'fecha_publicacion',
'sSearch_2':'',
'bRegex_2':'false',
'bSearchable_2':'true',
'bSortable_2':'false',
'mDataProp_3':'provincia',
'sSearch_3':'', 
'bRegex_3':'false',
'bSearchable_3':'true',
'bSortable_3':'false',
'mDataProp_4':'objeto_contratacion',
'sSearch_4':'',
'bRegex_4':'false',
'bSearchable_4':'true',
'bSortable_4':'false',
'mDataProp_5':'estado',
'sSearch_5':476,
'bRegex_5':'false',
'bSearchable_5':'true',
'bSortable_5':'true',
'mDataProp_6':'fecha_limite_propuesta',
'sSearch_6':'',
'bRegex_6':'false',
'bSearchable_6':'true',
'bSortable_6':'false',
'mDataProp_7':'url',
'sSearch_7':'',
'bRegex_7':'false',
'bSearchable_7':'true',
'bSortable_7':'false',
'mDataProp_8':'direccion_entrega',
'sSearch_8':'',
'bRegex_8':'false',
'bSearchable_8':'true',
'bSortable_8':'false',
'mDataProp_9':'contacto',
'sSearch_9':'',
'bRegex_9':'false',
'bSearchable_9':'true',
'bSortable_9':'true',
'sSearch':'',
'bRegex':'false',
'iSortCol_0':1,
'sSortDir_0':'desc',
'iSortingCols':1
}

page = requests.post(url,headers=headers,params=params, data=data)
dataWeb = page.json()

tope = int(dataWeb['iTotalDisplayRecords'])
tope

numregistros=0
incremento=20000
while numregistros <= tope:
    # ir sacando cada 100
    if numregistros==0:
            data={ 
            'sEcho':0,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54089,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':476,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )   
            print(tope,numregistros,incremento)
    else:
            data={ 
            'sEcho':1,
            'iColumns':10,
            'sColumns':'%2C%2C%2C%2C%2C%2C%2C%2C%2C',
            'iDisplayStart':numregistros+1,
            'iDisplayLength':incremento,
            'mDataProp_0':'tipo_necesidad',
            'sSearch_0':54089,
            'bRegex_0':'false',
            'bSearchable_0':'true',
            'bSortable_0':'false',
            'mDataProp_1':'codigo_contratacion',
            'sSearch_1':'', 
            'bRegex_1':'false',
            'bSearchable_1':'true',
            'bSortable_1':'true',
            'mDataProp_2':'fecha_publicacion',
            'sSearch_2':'',
            'bRegex_2':'false',
            'bSearchable_2':'true',
            'bSortable_2':'false',
            'mDataProp_3':'provincia',
            'sSearch_3':'', 
            'bRegex_3':'false',
            'bSearchable_3':'true',
            'bSortable_3':'false',
            'mDataProp_4':'objeto_contratacion',
            'sSearch_4':'',
            'bRegex_4':'false',
            'bSearchable_4':'true',
            'bSortable_4':'false',
            'mDataProp_5':'estado',
            'sSearch_5':476,
            'bRegex_5':'false',
            'bSearchable_5':'true',
            'bSortable_5':'true',
            'mDataProp_6':'fecha_limite_propuesta',
            'sSearch_6':'',
            'bRegex_6':'false',
            'bSearchable_6':'true',
            'bSortable_6':'false',
            'mDataProp_7':'url',
            'sSearch_7':'',
            'bRegex_7':'false',
            'bSearchable_7':'true',
            'bSortable_7':'false',
            'mDataProp_8':'direccion_entrega',
            'sSearch_8':'',
            'bRegex_8':'false',
            'bSearchable_8':'true',
            'bSortable_8':'false',
            'mDataProp_9':'contacto',
            'sSearch_9':'',
            'bRegex_9':'false',
            'bSearchable_9':'true',
            'bSortable_9':'true',
            'sSearch':'',
            'bRegex':'false',
            'iSortCol_0':1,
            'sSortDir_0':'desc',
            'iSortingCols':1
            }
            page = requests.post(url,headers=headers,params=params, data=data)
            dataWeb = page.json()
            dfDataWeb2=pd.DataFrame.from_records(dataWeb['data'])
            if (len(dfDataWeb2.index) > 0) : 
                dfDataWeb=pd.concat([dfDataWeb,dfDataWeb2], ignore_index=True, sort=False )   
            print(tope,numregistros,incremento)
    numregistros=numregistros+incremento
    
    dfDataWeb.count()
    
# Transformacion sobre los campos

today_ts = pd.Timestamp.today()
dfDataWeb['fecha_hoy'] = today_ts

dfDataWeb['fecha_hoy']= pd.to_datetime(dfDataWeb['fecha_hoy'].dt.date)
dfDataWeb['fecha_limite_propuesta']= pd.to_datetime(dfDataWeb['fecha_limite_propuesta'], format='%Y-%m-%d %H:%M:%S')
dfDataWeb['fecha_publicacion']= pd.to_datetime(dfDataWeb['fecha_publicacion'], format='%Y-%m-%d %H:%M:%S')

dfDataWeb['fecha_limite_propuesta_id']= dfDataWeb['fecha_limite_propuesta'].dt.strftime('%Y%m%d').astype(int)
dfDataWeb['fecha_publicacion_id']= dfDataWeb['fecha_publicacion'].dt.strftime('%Y%m%d').astype(int)

dfDataWeb['seq_tipo_necesidad'] = dfDataWeb['seq_tipo_necesidad'].astype(str).astype(int)
dfDataWeb['tcom_necesidad_contratacion_id'] = dfDataWeb['tcom_necesidad_contratacion_id'].astype(str).astype(int)
dfDataWeb['seq_estado'] = dfDataWeb['seq_estado'].astype(str).astype(int)

## Pilas hay que revisar por que se pierde la letra A en Adquision 
    
def normalize(s):
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
        ("?", ""),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s

def replaceURL(s):
    patron = patronReemplazo
    URLDetalle = url_relativa
    s = s.replace(patron, URLDetalle)
    return s


dfDataWeb['url'] = dfDataWeb['url'].apply(replaceURL)

patronReemplazo='<a href='
url_relativa=''

dfDataWeb['url'] = dfDataWeb['url'].apply(replaceURL)

patronReemplazo='</a>'
url_relativa=''


dfDataWeb['url'] = dfDataWeb['url'].apply(replaceURL)

dfDataWeb['url'] = dfDataWeb['url'].str.split('>',expand=True)[0]

dfDataWeb['url'] = dfDataWeb['url'].str.strip()


dfDataWeb['objeto_contratacion'] = dfDataWeb['objeto_contratacion'].apply(normalize)

dfDataWeb['razon_social'] = dfDataWeb['razon_social'].apply(normalize)

dfDataWeb['objeto_contratacion'] = dfDataWeb['objeto_contratacion'].str.upper()
dfDataWeb['razon_social'] = dfDataWeb['razon_social'].str.upper()

dfDataWeb['link'] = '[' + dfDataWeb['objeto_contratacion'] + ']' + '(' + dfDataWeb['url'] + ')' 

patronReemplazo='Funcionario Encargado:'
url_relativa=''

dfDataWeb['contacto'] = dfDataWeb['contacto'].apply(replaceURL)

patronReemplazo='Email: '
url_relativa=''

dfDataWeb['contacto'] = dfDataWeb['contacto'].apply(replaceURL)


dfDataWeb['emailcontacto'] = dfDataWeb['contacto'].str.split('<br/>',expand=True)[1]
dfDataWeb['contacto'] = dfDataWeb['contacto'].str.split('<br/>',expand=True)[0]


for column in dfDataWeb:
    print(column,"->", dfDataWeb[column].astype(str).str.len().max())
    


# Estandarizacion de datos


#contratado

#Estudio de mercado
#Necesidades de COntratacion

dfDataWeb['tipo_necesidad']=dfDataWeb['seq_tipo_necesidad'].replace([54089,54092],['Contrato','Infimas']) 



dfDataWeb['yearpub'] = pd.DatetimeIndex(dfDataWeb['fecha_publicacion']).year

dfDataWeb['monthpub'] = pd.DatetimeIndex(dfDataWeb['fecha_publicacion']).month

dfDataWeb['ciudad'] = dfDataWeb['provincia'].str.split('-',expand=True)[1]
dfDataWeb['ciudad'] = dfDataWeb['ciudad'].str.strip()

dfDataWeb['provincia'] = dfDataWeb['provincia'].str.split('-',expand=True)[0]
dfDataWeb['provincia'] = dfDataWeb['provincia'].str.strip()


dfDataWeb['cant_reg'] = 1


#  
# neceidad infima cuantia 

# estudio de mercado   tipo_necesidad   contratacion es un presupuesto un estudio no va a ser adjudicado

#en curso todavia puedes entregar proforma
# finalizado ya no se puede entregar proforma

#desierta es cuando nadie ha participado
# reemplazar desierta por "vencido" porque el plazo de entrega ya vencio.

#eliinar el tercer estado

#En entida contratante dejar el link de url



##  detalle de objeto de compra
#---------------------
# variable  ingreasdo por el solicitante
# esto ponerlo en una tabla  diferente  relacionada por el primer

# el No   es el codigo CPC  (catalogo de los servicios o compras q adquieren)
#  depende del RUC q se tiene para vender en el proveedor
# cada empresa le sale el código para poder vencer
# según el RUC y actividad comercial
# para otros tipos de compras públicas toca pedir habilitar ese CPC
# gestionar eso con el SERCOP
# el CPC es el nombre de ese codigo 


#  Forma de pago
#-----------------
#  contra entrega
#  x porcentaje de atnicipo  total menos el porcentaje e anticipo
#   Ver especificaciones
#La forma en que van a pagar al proveedor


# Documentos Anexos
#-------------------------
#ARchivo s que se deben de leer para ver q se va a solicitar

# Especificaciaones téncicas
# Informe de necesidad
# Presentacion de proforma

#  Proveedores (caundo ya sale finalizdo el proceso)
#-------------------------------
#  sale el listado de losque presentaron proforma
# puede haber finalizado que no tengn registros probablemetne vuelvan a publicar 

#


#en la malla poner un check para indicar que voy gestionar el envio de la proforma
#otro estado para indicar que ya envie la proforma


#entidad contratante (filtro de columna
# 
# 
# de 9 a 10 con Christina  Peñafiel  --- subasta inversa 


columnas_a_usar= ['codigo_contratacion','tcom_necesidad_contratacion_id','tipo_necesidad','fecha_limite_propuesta','fecha_publicacion','fecha_limite_propuesta_id','fecha_publicacion_id','provincia','canton','direccion_entrega',
                 'objeto_contratacion','link','razon_social','estado','ciudad','cant_reg','yearpub','monthpub','url','contacto','emailcontacto']

dfDataWeb=dfDataWeb[columnas_a_usar]


dfDataWeb['es_empresa_publica']=dfDataWeb['razon_social'].str.contains('EMPRESA|TELEC', na=False)
dfDataWeb['es_empresa_publica']=dfDataWeb['es_empresa_publica'].fillna(False)
dfDataWeb[dfDataWeb['es_empresa_publica'].isnull()]['es_empresa_publica']=False


dfDataWeb['es_ministerio']=dfDataWeb[dfDataWeb["razon_social"].str.contains("MINISTERIO", na=False)]['razon_social'][~dfDataWeb["razon_social"].str.contains("COORDINACION|DIRECCION", na=False)].str.contains('DEPORTE|AMBIENTE|CULTURA|AGUA|DEFENSA|DEPORTE|TELECOMUNICACIONES|TURISMO|PRODUCCION|PESA|COMERCIO|TRABAJO|TRANSPORTE|URBANO|DESARROLLO|ENERGIA|MINAS|INCLUSION|ECONOMICA|MUJER|DERECHOS|RELACIONES|MOVILIDAD|PRODUCCION|INVERSIONES|SOCIAL', na=False)
dfDataWeb['es_ministerio']=dfDataWeb['es_ministerio'].fillna(False)
dfDataWeb[dfDataWeb['es_ministerio'].isnull()]['es_ministerio']=False

dfDataWeb['es_coordinacion_ministerio']=dfDataWeb[dfDataWeb["razon_social"].str.contains("MINISTERIO|COORDINACION", na=False)]['razon_social'].str.contains('COORDINACION', na=False)
dfDataWeb['es_coordinacion_ministerio']=dfDataWeb['es_coordinacion_ministerio'].fillna(False)
dfDataWeb[dfDataWeb['es_coordinacion_ministerio'].isnull()]['es_coordinacion_ministerio']=False


dfDataWeb['es_secretaria_ministerio']=dfDataWeb[dfDataWeb["razon_social"].str.contains("MINISTERIO|NACIONAL|SECRETARIA", na=False)]['razon_social'].str.contains('SECRETARIA', na=False)
dfDataWeb['es_secretaria_ministerio']=dfDataWeb['es_secretaria_ministerio'].fillna(False)
dfDataWeb[dfDataWeb['es_secretaria_ministerio'].isnull()]['es_secretaria_ministerio']=False

dfDataWeb['es_sector_deporte']=dfDataWeb[~dfDataWeb["razon_social"].str.contains("MINISTERIO", na=False)]['razon_social'].str.contains('DEPORT|NATACION|FUTBOL|PING|TENNIS|NATACION|VOLLEY|OLIMPIC', na=False)
dfDataWeb['es_sector_deporte']=dfDataWeb['es_sector_deporte'].fillna(False)
dfDataWeb[dfDataWeb['es_sector_deporte'].isnull()]['es_sector_deporte']=False

dfDataWeb['es_gobierno_autonomo']=dfDataWeb['razon_social'].str.contains('GOBIERNO AUTONOMO|GOBIERNO PROVINCIAL|JUNTA PARROQUIAL', na=False)
dfDataWeb['es_gobierno_autonomo']=dfDataWeb['es_gobierno_autonomo'].fillna(False)
dfDataWeb[dfDataWeb['es_gobierno_autonomo'].isnull()]['es_gobierno_autonomo']=False

dfDataWeb['es_sector_seguridad']=dfDataWeb['razon_social'].str.contains('BOMBERO|POLICIA|MILITAR', na=False)
dfDataWeb['es_sector_seguridad']=dfDataWeb['es_sector_seguridad'].fillna(False)
dfDataWeb[dfDataWeb['es_sector_seguridad'].isnull()]['es_sector_seguridad']=False

dfDataWeb['es_sector_salud']=dfDataWeb['razon_social'].str.contains('CENTRO CLINICO|CLINICA|HOSPITAL', na=False)
dfDataWeb['es_sector_salud']=dfDataWeb['es_sector_salud'].fillna(False)
dfDataWeb[dfDataWeb['es_sector_salud'].isnull()]['es_adquisicion_material']=False

dfDataWeb['es_sector_aereo']=dfDataWeb['razon_social'].str.contains('AEROPORTUARIO|AEROPUERTO', na=False)
dfDataWeb['es_sector_aereo']=dfDataWeb['es_sector_aereo'].fillna(False)
dfDataWeb[dfDataWeb['es_sector_aereo'].isnull()]['es_sector_aereo']=False

# Filtros para criterio de busqueda en objeto de contratacion
dfDataWeb['es_adquisicion_material']=dfDataWeb['objeto_contratacion'][~dfDataWeb["objeto_contratacion"].str.contains("REPUESTO")].str.contains('ADQUISICION|COMPRA', na=False)
dfDataWeb['es_adquisicion_material']=dfDataWeb['es_adquisicion_material'].fillna(False)
dfDataWeb[dfDataWeb['es_adquisicion_material'].isnull()]['es_adquisicion_material']=False

dfDataWeb['es_servicio_tecnico']=dfDataWeb[~dfDataWeb["objeto_contratacion"].str.contains("CONTRATACION|MEDICO|ALQUILER|PROFESIONA|CULTURA|VEHICULO|MANTENIMIENTO|TRANSPORTE|ARRENDAMIENTO|ADQUISICION", na=False)]["objeto_contratacion"].str.contains(r'(?=.*SERVICIO TECNICO)(?=.*TECNICO)', na=False,regex=True)
dfDataWeb['es_servicio_tecnico']=dfDataWeb['es_servicio_tecnico'].fillna(False)


dfDataWeb['es_servicio_mantenimiento']=dfDataWeb['objeto_contratacion'].str.contains(r'(?=.*SERVICIO)(?=.*MANTENIMIENTO)', na=False,regex=True).fillna(False)


dfDataWeb['es_correctivo']=dfDataWeb['objeto_contratacion'].str.contains('CORRECTIVO', na=False).fillna(False)


dfDataWeb['es_repuesto']=dfDataWeb['objeto_contratacion'].str.contains('REPUESTO', na=False).fillna(False)


dfDataWeb['es_compras_salud']=dfDataWeb['objeto_contratacion'][~dfDataWeb["objeto_contratacion"].str.contains("RESPIRATORIO|NERVIOSO|SANGUINEO|LINFATICO|OSEO|MUSCULO", na=False)][dfDataWeb["objeto_contratacion"].str.contains("HOSPITAL", na=False)].str.contains('SISTEMA', na=False)
dfDataWeb['es_compras_salud']=dfDataWeb['es_compras_salud'].fillna(False)


dfDataWeb['es_otros_salud']=dfDataWeb['objeto_contratacion'][dfDataWeb["objeto_contratacion"].str.contains("RESPIRATORIO|NERVIOSO|SANGUINEO|LINFATICO|OSEO|MUSCULO", na=False)][dfDataWeb["objeto_contratacion"].str.contains("HOSPITAL", na=False)].str.contains('SISTEMA', na=False)
dfDataWeb['es_otros_salud']=dfDataWeb['es_otros_salud'].fillna(False)

dfDataWeb['es_software']=dfDataWeb['objeto_contratacion'].str.contains('APLICACION|SOFTWARE', na=False)

dfDataWeb['es_software']=dfDataWeb['es_software'].fillna(False)


dfDataWeb['es_equipos']=dfDataWeb[~dfDataWeb["objeto_contratacion"].str.contains("HOSPITAL|MEDICO|CLINICA", na=False)]['objeto_contratacion'].str.contains('EQUIPOS', na=False)
dfDataWeb['es_equipos']=dfDataWeb['es_equipos'].fillna(False)


dfDataWeb['es_insumo']=dfDataWeb['objeto_contratacion'][~dfDataWeb["objeto_contratacion"].str.contains("HOSPITAL|MEDICO|CLINICA")].str.contains('INSUMO', na=False)
dfDataWeb['es_insumo']=dfDataWeb['es_insumo'].fillna(False)


dfDataWeb['es_contratacion_servicios']=dfDataWeb['objeto_contratacion'][~dfDataWeb["objeto_contratacion"].str.contains("SERVICIOS|CONSULTORIA")].str.contains('CONTRATACION', na=False)
dfDataWeb['es_contratacion_servicios']=dfDataWeb['es_contratacion_servicios'].fillna(False)


dfDataWeb['dia_semana'] = dfDataWeb['fecha_publicacion'].dt.day_of_week
import calendar
dfDataWeb['monthnamepub'] = dfDataWeb['monthpub'].apply(lambda x: calendar.month_abbr[x])



#Crear datos de meses en la data


#respaldo local de la ulima extraccion en archivos.
#dfDataWeb.to_csv('dataComprasPublicas.csv')

#dfDataWeb.to_pickle('dataComprasPublicas.pkl')

# dfDataWeb[['estado','cant_reg']].groupby(['estado']).sum('cant_reg')



dfDataWebAyer=dfDataWeb

# filtrar por el dia de ayer para que solo inserte el corte de ayer 

# dfDataWebAyer=dfDataWeb[dfDataWeb['fecha_publicacion'].between(str_fecha_ayer,str_fecha_hoy)]

#conn_string = 'postgresql://postgres:admin123@172.20.0.112:5433/infy_projects'
conn_string = 'postgresql://postgres:imaiden123@localhost:5432/infy_projects'
  
  


db = create_engine(conn_string)
conn = db.connect()



# Create DataFrame

dfDataWebAyer.to_sql('web_scrapping_gov', con=conn, if_exists='append',
          index=False)


conn = psycopg2.connect(conn_string
                        )
conn.autocommit = True
cursor = conn.cursor()
  
sql1 = '''select * from web_scrapping_gov limit 1;'''
cursor.execute(sql1)
for i in cursor.fetchall():
    print(i)
  
  
conn.commit()
conn.close()


dfDataWebAyer=dfDataWeb[dfDataWeb['fecha_publicacion'].between(str_fecha_ayer,str_fecha_hoy)]
dfDataWebAyer=dfDataWebAyer[dfDataWebAyer['estado'] =='En Curso']


message = """From:sistema_infimas_cuantias
To: cesar.villarroel<cesar.villarroel@compsesa.com.ec>
Subject: """  + " Existen:" +  """  oportunidades de infimas cuantias\n\n
"""  + "Existen: {}".format(str(len(dfDataWebAyer))) +  """  oportunidades de infimas cuantias"""



if ( len(dfDataWebAyer) == 0 ):
   print("No hay infimas cuantias para el dia de hoy") 
else:
    sender = 'cesar.villarroel@compsesa.com.ec'
    receivers = ['cesar.villarroel@compsesa.com.ec']
    try:
        smtpObj = smtplib.SMTP('mail.compsesa.com.ec')
        smtpObj.sendmail(sender, receivers, message)         
        print ("Correo enviado sin novedades")
    except smtplib.SMTPException:
        print ("Error: no se puede enviar el correo")
   
