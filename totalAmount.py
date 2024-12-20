from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf = conf)

def extract(line):
    cust = line.split(',')[0]
    amt = line.split(',')[2]
    return (cust, float(amt))

input = ("ex-customer-orders.csv")
lines = sc.textFile(input)

cust_amount = lines.map(extract)

cust_amount1 = cust_amount.reduceByKey(lambda x, y: x + y)
sortedbyAmount = cust_amount1.map(lambda x: (x[1], x[0])).sortByKey()
results = sortedbyAmount.collect()


for result in results:
    amt = str(result[0])
    cust = result[1]
    print(cust + " : " + amt)

