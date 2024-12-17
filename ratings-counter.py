from pyspark import SparkConf, SparkContext
import collections

def extract(x):
    return x.split()[2]


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
# ratings = lines.map(lambda x: x.split()[2])
ratings = lines.map(extract)

result = ratings.countByValue()
print(result)

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s  - %i" % (key, value))



