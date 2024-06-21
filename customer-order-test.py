from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('customer-order-test').getOrCreate()
schema = StructType([
    StructField('cust_id',IntegerType(),True),
    StructField('item_id', IntegerType(), True),
    StructField('price', FloatType(), True),
])
customer = spark.read.schema(schema).csv('customer-orders.csv')
customer.printSchema()
customer.show()
customer.groupby('cust_id').sum('price').withColumn('price',func.round(func.col("sum(price)"),2)).sort('cust_id').\
    select('price','cust_id').show()
spark.stop()