from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()

conf.set('spark.executor.memory', '16g')
conf.set('spark.executor.cores', '4')
conf.set('spark.driver.memory', '6g')
conf.set('spark.memory.fraction', '0.8')
conf.set('spark.executor.instances','5')


conf.setAppName("spark-conf-AS")

spark = SparkSession.builder\
  .config(conf = conf)\
  .enableHiveSupport()\
  .getOrCreate()
