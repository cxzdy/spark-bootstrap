from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType


if __name__ == "__main__":
    sc = SparkContext(appName="SparkSQL:[com.mvad.spark.demo][pysparkdemo]")
    sqlContext = HiveContext(sc)

    # RDD is created from a list of rows
    df = sqlContext.parquetFile("/mvad/warehouse/session/dspan/date=2015-05-01/")
    df.registerTempTable("sessionlog")
    for table in sqlContext.tableNames():
        print table
    df.printSchema()

    sqlContext.udf.register("intarr2str",lambda array:"".join(map(str,array)) )
    sql1 = """ select intarr2str(cookie) as cookiestr,eventTime,eventType,geoInfo.country as country,
      geoInfo.province as province from sessionlog limit 10 """.replace('\n',' ')
    sample = sqlContext.sql(sql1)
    sample.show()


    sql2 = """select eventType, count(cookie) as count from sessionlog
      group by eventType """.replace('\n',' ')
    result = sqlContext.sql(sql2)
    result.cache()

    # only show 20 records
    result.show()
    result.show(100)

    # collect to driver for post processing
    for row in result.collect():
        print "%s,%s" %(row.asDict().get("count"),row.asDict().get("eventType"))

    # save result
    result.saveAsParquetFile("/tmp/result-parquet")
    result.rdd.saveAsTextFile("/tmp/result-txt")

    # bug api, only create to default DB
#    result.saveAsTable("result")
#    result.insertInto("result")

    sc.stop()
