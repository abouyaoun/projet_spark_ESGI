import pyspark.sql.functions as f
from pyspark.sql.functions import col, substring, when
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("e") \
        .master("local[*]") \
        .getOrCreate()
    
    df1 = spark.read.option('header', True).csv("/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo2/city_zipcode.csv")
    df2 = spark.read.option('header', True).csv("/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo2/clients_bdd.csv")
    df_1_2 = df1.join(df2, df1["zip"] == df2["zip"], "inner")
    df_1_2 = df_1_2.drop(df2["zip"])

    df_majeur = ageMajeur(df_1_2)

    df_1_2 = df_1_2.withColumn("departement",
                                when((col("zip").substr(1, 5) <= "20190"), "2A")
                                .when((col("zip").substr(1, 5) > "20190") & (col("zip").substr(1, 5 ) < "21000"), "2B")
                                .otherwise(substring(col("zip"), 1, 2)))
    
    
    df_1_2.write.mode("overwrite").parquet("/Users/ayman/vscode/sparkexo0/spark-handson/src/fr/hymaia/exo2/clean")

    
def ageMajeur(df):
    return df.filter(col("age") >=18) 
