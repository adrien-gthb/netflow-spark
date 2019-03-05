from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.appName("sne_nf_converter").getOrCreate()
sc = spark.sparkContext
sc.addPyFile("hdfs://****/projects/sne_nf/IPy.py")
import IPy
from IPy import IP


CSV_DIR = '/projects/sne_nf/test_data/csv'
PARQUET_DIR = '/projects/sne_nf/test_data/parquet'
CSV_HEADER = 'ts,te,td,sa,da,sp,dp,pr,flg,fwd,stos,ipkt,ibyt,opkt,obyt,in,out,sas,das,smk,dmk,dtos,dir,nh,nhb,svln,dvln,ismc,odmc,idmc,osmc,mpls1,mpls2,mpls3,mpls4,mpls5,mpls6,mpls7,mpls8,mpls9,mpls10,cl,sl,al,ra,eng,exid,tr'


def _IPtoInt(addr):
    try:
        return IP(addr).int()
    except:
        return None

def csvToParquet():
    df_csv = spark \
        .read \
        .option('header', 'false') \
        .option('delimiter', ',') \
        .option('inferSchema', 'true') \
        .format('csv') \
        .load(CSV_DIR + '/*') \
        .toDF(*CSV_HEADER.split(','))
    pruned_df = df_csv.select(
        'ts', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg', 'ipkt', 'ibyt', 'opkt', 'obyt')
#    udf_func = udf(_IPtoInt, LongType())
    df = pruned_df.withColumn('ts', unix_timestamp(
        pruned_df['ts'], 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))
#    df2 = (df.withColumn('td', df['td'].cast('double')).withColumn('sp', df['sp'].cast('int')).withColumn('dp', df['dp'].cast('int')).withColumn('ipkt', df['ipkt'].cast('int')).withColumn(
#        'ibyt', df['ibyt'].cast('int')).withColumn('opkt', df['opkt'].cast('int')).withColumn('obyt', df['obyt'].cast('int')).withColumn('sa', udf_func('sa')).withColumn('da', udf_func('da')))
    df2 = (df.withColumn('td', df['td'].cast('double')).withColumn('sp', df['sp'].cast('int')).withColumn('dp', df['dp'].cast('int')).withColumn('ipkt', df['ipkt'].cast('int')).withColumn(
        'ibyt', df['ibyt'].cast('long')).withColumn('opkt', df['opkt'].cast('int')).withColumn('obyt', df['obyt'].cast('long')))
    df2.write.parquet(PARQUET_DIR)


if __name__ == '__main__':
    csvToParquet()
    sc.stop()
