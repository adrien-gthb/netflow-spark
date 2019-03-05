from pyspark.sql import SparkSession 
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import sys
import time
import datetime

spark = SparkSession.builder.appName("sne_nf_querier").getOrCreate()
sc = spark.sparkContext
sc.addPyFile("hdfs://****/projects/sne_nf/IPy.py")
import IPy
from IPy import IP


PARQUET_DIR = '/projects/sne_nf/test_data/parquet'
CSV_HEADER = 'ts,te,td,sa,da,sp,dp,pr,flg,fwd,stos,ipkt,ibyt,opkt,obyt,in,out,sas,das,smk,dmk,dtos,dir,nh,nhb,svln,dvln,ismc,odmc,idmc,osmc,mpls1,mpls2,mpls3,mpls4,mpls5,mpls6,mpls7,mpls8,mpls9,mpls10,cl,sl,al,ra,eng,exid,tr'
PARQUET_COL = ['ts', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg', 'ipkt', 'ibyt', 'opkt', 'obyt']


def _IPtoInt(addr):
    try:
	return IPy.IP(addr).int()
    except:
        return None

def _intToIP(i):
    try:
	return IPy.IP(i).strNormal()
    except:
        return ''

def _getIPversion(i):
    try:
	return IPy.IP(i).version()
    except:
        return 0

def query():
    parquetFile = spark.read.load(PARQUET_DIR + '/*.parquet')
    parquetFile.createOrReplaceTempView('parquetFile')
    #sqlContext.registerFunction("IntToIP", _intToIP, StringType())
    #sqlContext.registerFunction("IPtoInt", _IPtoInt, LongType())
    #sqlContext.registerFunction("IPversion", _getIPversion, IntegerType())

    # Timestamp filtering
    #ts_from = '2018-01-25 08:30:00'
    #ts_from = '2018-01-25 08:00:00'
    #ts_from = '2018-01-25 05:30:00'
    ts_from = '2018-01-25 02:02:30'
    #ts_to = '2018-01-25 08:35:00'
    #ts_to = '2018-01-25 09:00:00'
    ts_to = '2018-01-25 09:02:30'

    # From 4.5 hours earlier to now
    #ts_from = (datetime.datetime.now() - datetime.timedelta(hours=4.5)).strftime('%Y-%m-%d %H:%M:%S')
    #ts_to = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    # Select all
    #qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt FROM parquetfile'
    #qry = 'SELECT ts, td, da, ipkt, opkt, ibyt, obyt FROM parquetfile'

    # IP querying
    '''
    #ip = IPy.IP('127.0.0.1').int()
    ip = '127.0.0.1'
    #ip = '::1'
    qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt FROM parquetFile WHERE (ts >= CAST("{0}" AS TIMESTAMP) AND ts <= CAST("{1}" AS TIMESTAMP)) AND (sa = "{2}" OR da = "{3}")'.format(ts_from, ts_to, ip, ip)
    #qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt FROM parquetFile WHERE da = "{0}"'.format(ip)
    '''

    # Select IPs receiving most traffic
    #qry = 'SELECT MIN(ts), SUM(td), da, COUNT(da), SUM(ipkt), SUM(opkt), SUM(ibyt), SUM(obyt) FROM parquetFile WHERE (ts >= CAST("{0}" AS TIMESTAMP) AND ts <= CAST("{1}" AS TIMESTAMP)) GROUP BY da ORDER BY SUM(ibyt) DESC'.format(ts_from, ts_to)

    # Records with bytes > 100M
    #qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt, ibyt+obyt AS bytes FROM parquetFile WHERE (ts >= CAST("{0}" AS TIMESTAMP) AND ts <= CAST("{1}" AS TIMESTAMP)) AND ibyt+obyt > 100000000'.format(ts_from, ts_to)

    # IPvX only
    #qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt FROM parquetFile WHERE IPversion(sa) = 6'

    # TCP SYN flag only
    #qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt FROM parquetFile WHERE (ts >= CAST("{0}" AS TIMESTAMP) AND ts <= CAST("{1}" AS TIMESTAMP)) AND pr = "TCP" AND flg = "....S."'.format(ts_from, ts_to)

    # Telnet SYN packets order by bps
    qry = 'SELECT ts, td, sa, sp, da, dp, pr, flg, ipkt, opkt, ibyt, obyt, ((ibyt+obyt)/td)*8 AS bps FROM parquetFile WHERE (ts >= CAST("{0}" AS TIMESTAMP) AND ts <= CAST("{1}" AS TIMESTAMP)) AND (sp IN(23, 992) OR dp IN(23, 992)) AND flg = "....S." ORDER BY (((ibyt+obyt)/td)*8) DESC'.format(ts_from, ts_to)

    # Execute query
    df = spark.sql(qry).limit(10)

    # Drop view
    spark.catalog.dropTempView("parquetFile")
    
    df.persist(StorageLevel.MEMORY_ONLY)
    #print df.printSchema()
    print df.count()
    df.show(20, False)
    df.unpersist() 


if __name__ == '__main__':
    query()
    sc.stop()
