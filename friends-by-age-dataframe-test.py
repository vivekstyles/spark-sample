# from pyspark.sql import SparkSession
# from pyspark.sql import Row
#
# spark = SparkSession.builder.appName("Friends-age-dataFrame").getOrCreate()
#
#
# def mapper(line):
#     column = line.split(',')
#     return Row(id=column[0], name=column[1], age=column[2], friends=column[3])
#
#
# line = spark.sparkContext.textFile('fakefriends.csv')
# people = line.map(mapper)
#
# schemaPeople = spark.createDataFrame(people).cache()
# schemaPeople.createOrReplaceTempView('people')
#
# teenagers = spark.sql("select age,count(age),avg(friends) from people group by age order by age")
# for i in teenagers.collect():
#     print(i)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Friends-age-dataFrame").getOrCreate()

line = spark.read.option('header','true').option('inferSchema','true').csv('fakefriends-header.csv')
print(line.select('age','friends').groupby('age').avg('friends').orderBy('age').show())
spark.stop()