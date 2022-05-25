from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import concat
from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import col, expr, when
import argparse
from pyspark.sql.functions import explode

def flat_paragraph(record):
  context = record['paragraphs'][0]['context']
  qas = record['paragraphs'][0]['qas']
  sources=[]
  context_len=len(context)
  index = 0
  res=[]
  while(index < context_len):
    end = min(index+512,context_len)
    sources.append(context[index:end])
    for question in qas:
      flag=True
      if question['is_impossible']==False:
        for answer in question['answers']:
          if answer['answer_start']>=index and answer['answer_start']<end:
            flag=False
            res.append((record['title'],context[index:end],question['question'],answer['answer_start'],min(end,answer['answer_start']+len(answer['text']))))
        if flag:
          res.append((record['title'],context[index:end],question['question'],0,0))
      else:
        res.append((record['title'],context[index:end],question['question'],0,0))
    index+=256

  return res


parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='week8_out')
args = parser.parse_args()
output_path = args.output

spark_conf = SparkConf()\
    .setAppName("Assignment 2")
sc=SparkContext.getOrCreate(spark_conf) 
text_file='CUADv1.json'
spark = SparkSession \
    .builder \
    .appName("Assignment 2: Spark Data Analytics") \
    .getOrCreate()
df = spark.read.format("json").load(text_file)
df_data = df.select((explode("data").alias('data')))
df_data = df_data.select((explode("data.paragraphs").alias("paragraph")),'data.title')
df_data = df_data.select((explode("paragraph.qas").alias("qas")),"paragraph.context","title")
# contract_rdd = df_data.rdd
contract_rdd = sc.parallelize(df.select("data").first()['data'])
sample_rdd = contract_rdd.flatMap(flat_paragraph)
samples_df = spark.createDataFrame(sample_rdd,['title', 'source','question','answer_start','answer_end'])
sample_df = samples_df.withColumn('title_question',concat(samples_df['title'],samples_df['question']))
sample_df.show(5)
title_question_count=sample_df.filter('answer_start!=0').groupby('title_question','title','question').count().withColumnRenamed('count','positive_count')
question_avg=title_question_count.groupby('question').agg({'positive_count':'avg'}).withColumnRenamed('avg(positive_count)','question_avg')
title_question_0_count=sample_df.filter('answer_start=0').groupby('title_question','title','question').count().withColumnRenamed('count','negative_count')
big_df = title_question_0_count.join(title_question_count,['title_question','title','question'],"left").join(question_avg,['question'],"left")
# big_df.show(5)
new_column_2 = when(
    col("positive_count").isNull(),col('question_avg')/col('negative_count')
    ).otherwise(col('positive_count')/col('negative_count'))
big_df = big_df.withColumn('ratio', new_column_2)
big_df.show(5)
new_column_3 = when(
    col("ratio").isNull(),0
    ).when(col('ratio')>1,1).otherwise(col('ratio'))
big_df = big_df.withColumn('ratio', new_column_3)
fractions = big_df.select('title_question','ratio').rdd.collectAsMap()
negative_samples = sample_df.filter('answer_start=0').sampleBy('title_question',fractions=fractions,seed=1)
positive_samples = sample_df.filter('answer_start!=0')
all_samples = positive_samples.union(negative_samples)
all_samples.show(5)
all_samples.select('source','question','answer_start','answer_end').write.json(output_path)