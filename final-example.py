from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import concat
from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import col, expr, when
import argparse
from pyspark.sql.functions import explode



spark = SparkSession \
    .builder \
    .config("spark.sql.shuffle.partitions",20)\
    .appName("COMP5349 2021 Exam") \
    .getOrCreate()
tweets_data = 'tweets.json'
tweets_df = spark.read.option('multiline','true').json(tweets_data)
parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='week8_out')
args = parser.parse_args()
output_path = args.output
tdf = tweets_df.select('id','replyto_id','retweet_id')

retweets = tdf \
          .select('id','retweet_id')\
          .filter(tdf.retweet_id.isNotNull())\
          .withColumnRenamed('retweet_id','tweet_id')

replies = tdf \
          .select('id','replyto_id')\
          .filter(tdf.replyto_id.isNotNull())\
          .withColumnRenamed('replyto_id','tweet_id')

t2_df = replies.union(retweets)
t3_df = t2_df.groupBy('tweet_id')\
              .count()\
              .withColumnRenamed('count','cnumber')

r1 = t3_df.sort(t3_df.cnumber.desc()).write.json(output_path)