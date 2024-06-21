from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer_spend")


def parsevalue(line):
    data = line.split(',')
    return int(data[0]), float(data[2])


sc = SparkContext(conf=conf)
lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parsevalue).reduceByKey(lambda a, b: (a + b))

for i in sorted(rdd.map(lambda x: (x[1],x[0])).collect(), reverse=True):
    print(i[1], ',', i[0])
