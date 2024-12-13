import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("first job") \
        .master("local[*]") \
        .getOrCreate()
    #ouvre un spark reader
    #préciser type
    df = spark.read.option('header', True).csv("/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo1/data.csv")
    df_wordcount = wordcount(df, 'text')
    df_wordcount.write.mode('overwrite').partitionBy('count').parquet('data/exo1/output')

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
