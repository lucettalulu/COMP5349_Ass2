from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,IntegerType,FloatType
from operator import add



spark = SparkSession \
    .builder \
    .appName("COMP5349 2021 Exam") \
    .getOrCreate()

rating_data = 'ratings.csv'
rating_schema = StructType([
    StructField('uid',IntegerType(),True),
    StructField('mid',IntegerType(),True),
    StructField('rate',FloatType(),True),
    StructField('ts',IntegerType(),True),
])

def func1(r):
    u=list(r)
    l=len(u)
    if l>100&l<=1:
        return []
    su = sorted(u)
    results=[]
    for i  in range(l):
        for j in range(i+1,l):
            results.append(((su[i],su[j]),1))
    return results
ratings = spark.read.csv(rating_data,header=False,schema=rating_schema)
var1 = ratings.rdd.map(lambda r:(r.mid,r.uid)).cache()
var2 = var1.count()
var3 = var1.groupByKey().values().flatMap(func1)
var4 = var3.reduceByKey(add).sortBy(lambda r:r[1],ascending=False).take(5)