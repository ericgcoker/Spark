from sqlalchemy import *
import numpy as np
import pandas as pd
import warnings
import os,sys
import subprocess
import pytz
import datetime
from datetime import timedelta
import traceback
import os
import warnings
warnings.filterwarnings('ignore')
##Simple script to create a Python list and use Parameters in SQLAlchemy to get a smaller subset of queries out a large redshift table
try:
	red = create_engine('postgresql://user_ro:{pass}@server.redshift.amazonaws.com:5439/sys',pool_recycle=3600)
	red.dialect.server_side_cursors = True
	red.execution_options(stream_results = True)
	sql = text("""SELECT distinct tail FROM xdw.process WHERE measure in ('relevant')""")
	craft = pd.read_sql_query(sql,con=red)

	dicts = dict()
	otails = craft['tail'].unique().tolist()
	otails1 = otails[:50]
	otails2 = otails[101:201]
## Use chunking to bring in bits and not HAVE to use all memory of machine to query and write
	query = text("""SELECT distinct tail,met1,"time",,met2,met3 FROM op.kan WHERE tail in :otails1 AND ("time" BETWEEN '2018-03-18 00:00:00' AND '2018-03-18 23:59:59')
	AND (met5 > 29000)
	""")
	think = pd.read_sql_query(query,con=red,params=dicts,chunksize=200000)
	for df in think:
		thinks = pd.concat([df])
	thinks.to_csv('/data4/think_0318-1.csv',index=False)
	query = text("""SELECT distinct tail,met1,"time",,met2,met3 FROM op.kan WHERE tail in :otails2 AND ("time" BETWEEN '2018-03-18 00:00:00' AND '2018-03-18 23:59:59')
	AND (met5 > 29000)
	""")
	think = pd.read_sql_query(query,con=red,params=dicts,chunksize=200000)
	for df in think:
		thinks = pd.concat([df])
	thinks.to_csv('/data4/think_0318-2.csv',index=False)

	from pushbullet import Pushbullet
	pb = Pushbullet('o.key')
	pb.push_note('local','is_done')
except:
	from pushbullet import Pushbullet
	ex = str(traceback.print_exc())
	pb.push_note(ex,'was_a_problem')	