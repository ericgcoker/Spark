import datetime
import pytz
import os
import time
import sys
import string
from datetime import date, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

from scp import SCPClient
from pyspark.sql import functions as F

from subprocess import call

APP_NAME = "con"
sc = SparkContext("", APP_NAME)
sc.setLogLevel("WARN")
sqlContext = HiveContext(sc)
sqlContext.setConf("spark.sql.parquet.binaryAsString","true")
PROD = "12.333.201.21"

## Loop through DATE partitions
yesterday = datetime.datetime.now(pytz.timezone('US/Central')).date() - timedelta(1)
dayte = str(yesterday.strftime("%Y") + '-' + yesterday.strftime("%m") + '-' + yesterday.strftime("%d"))
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
# start_date = date(2016, 11, 0)
# end_date = date(2016, 11, 1)
start_date = date.today() - timedelta(45)
end_date = date.today() - timedelta(4)

##CREATE simple loop to create csv from individual Parquet Data Partitions. These will later be concatenated using GLOB and deleted.
for single_date in daterange(start_date, end_date):
    dayte = single_date.strftime("%Y_")+str(single_date.month)+"_"+str(single_date.day)
    ## first do 2ku
    console = sqlContext.read.parquet("hdfs://" + PROD + "/data/canonical/year=" + str(single_date.year) + "/month=" +str(single_date.month) + "/day=" + str(single_date.day) + "/source=con")
    console.registerTempTable("temp")
    reply = sqlContext.sql("""SELECT distinct id,date_time,value FROM temp WHERE (value LIKE '%Transition to Above%' OR value LIKE '%Transition to ABOVE%') AND id NOT
    	LIKE '1%'""")
reply.toPandas().to_csv('/data4/console_'+str(dayte)+'_asa.csv',index=False)
