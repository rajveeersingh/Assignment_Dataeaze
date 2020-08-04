from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
# from pyspark.sql.session import SparkConf
spark = SparkSession.builder\
 .config("spark.sql.shuffle.partitions",2)\
 .appName("assignment")\
 .master("local[4]")\
 .getOrCreate()

spark_rdd =spark.read\
 .option("header","true")\
 .csv("/home/superadmin/DBDA/project/OYD/"
  "Company_assignments/nonConfidential.csv")



spark_parquat = spark.read.parquet("/home/superadmin/DBDA/project/OYD/Company_assignments/confidential.snappy.parquet")


spark_leed=spark_rdd.filter("City like 'Virginia'")\
        .groupby("LEEDSystemVersionDisplayName").count()\
        .withColumnRenamed("LEEDSystemVersionDisplayName","LED Projects          ")\
        .withColumnRenamed("count","No. of projects for virginia")\
        .orderBy(asc('count'))

spark_owner=spark_rdd.select("ProjectTypes","City","LEEDSystemVersionDisplayName","OwnerTypes")\
        .filter("City like 'Virginia'")\
        .groupby("OwnerTypes").count() \
        .withColumnRenamed("OwnerTypes", "OwnerTypes          ") \
        .withColumnRenamed("count", "No. of projects for virginia") \
        .orderBy(asc('count'))

spark_GSF=spark_rdd.select("City","IsCertified","GrossSqFoot")\
        .filter("City like 'Virginia'")\
        .groupby("City","IsCertified").count()\
        .withColumnRenamed("IsCertified","LED Certified          ")\
        .withColumnRenamed("count","No. of projects as per certifiaction")\
        .orderBy(asc('count'))

spark_zip=spark_rdd.select("ProjectName","Zipcode","City")\
        .filter("City like 'Virginia'")\
        .groupby("City","Zipcode").count()\
        .withColumnRenamed("Zipcode","ZipCode          ")\
        .withColumnRenamed("count","project for zip code")\
        .orderBy(asc('count'))


spark_leed.show()
spark_owner.show()
spark_GSF.show()
spark_zip.show()