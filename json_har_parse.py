import pandas as pd
import json
import sys
import requests
import simplejson
from harparser import HAR
from json import loads
from datetime import datetime
import os
import time
from sqlalchemy import *
import pandas as pd
import traceback
import scandir
from datetime import date, timedelta
engine2 = create_engine("mysql+mysqldb://readonly:pass@127.0.0.1:3306/sys",pool_recycle=3600)

##GET Relevant to subset and create necessary directory for files
hal = pd.read_sql_query("""SELECT Tail tail FROM sys.relevant_ones""",con=engine2)
tails = hal['tail'].unique().tolist()

directory = []
for x in tails:
    try:
        directory.append('/home/har/'+str(x))
    except:
        traceback.print_exc()
        continue
# print(directory)
## Define the existing ones and be sure to not re-insert existing records from MySQL
try:
    existing = pd.read_sql_query("""SELECT tail,date FROM ecoker.har""",con=mysqlengine)
    existing['ids'] = existing['tail'].map(str) + '_' +  existing['date'].map(str) + '_' + existing['id'].map(str) + '_' + existing['page'].map(str)
except:
    existing = pd.DataFrame()
    existing['ids'] = ''
    pass

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
start_date = date.today() - timedelta(30)
end_date = date.today() - timedelta(0)

for fi in directory:
    try:
        for x in os.listdir(fi):
            f = str(fi) + '/' + str(x)
            with open(f) as jsonfile:
                data1 = json.load(jsonfile)
                logs = data1['log']
                pages = logs['pages']
            newdict={}
            for k,v in [(key,d[key]) for d in pages for key in d]:
                if k not in newdict:
                    newdict[k]=[v]
                else:
                    newdict[k].append(v)
            pddict={}
            cols = ['_domInteractive','_loadTime','_fullyLoaded','_responses_404','_URL','id','requests']
            for k,v in newdict.iteritems():
                if k in cols:
                    pddict.update({k:v})
                else:
                    pass
        ###
            try:
                newdict = data1['log']['entries'][0]['response']['headers']

                for item in newdict:
                    if item['name'] == 'Date':
                        my_item = item['value']
                        break
                    else:
                        my_item = None
            except:
                my_item = ''
                pass

            frame = (pd.DataFrame.from_dict(pddict, orient='index', dtype=None)).transpose().fillna(0.0)
            dates = (datetime.strptime((x.split("_")[4].replace(".har","")), '%Y-%m-%d-%H-%M-%S')).strftime('%Y-%m-%d %H:%M:%S')
            frame['date'] = dates
            frame['tail'] = f.split('/WebPageTest')[0].replace('/home/har/','')
            frame['page'] = f.split('/HAR_FILE')[1].split('_')[0].replace('/','')
            frame['dt'] = my_item
            frame['dt'] = pd.to_datetime(frame['dt'])
            frame['tail'] = frame['tail'].str.strip()
    except:
        traceback.print_exc()
        continue
mask = ~frame['ids'].isin(existing['ids'])
export = frame.loc[mask]
del export['ids']
print(export.info())
##Insert newly parsed har records into MySQL Table
export.to_sql('har',con=mysqlengine,schema='ecoker',index=False,if_exists='append',chunksize=100000)