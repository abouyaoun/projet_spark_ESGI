from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    spark = SparkSession.builder \
        .appName("Second job") \
        .master("local[*]") \
        .getOrCreate()
    
    df_clean = spark.read.parquet('/Users/ayman/vscode/sparkexo0/spark-handson/src/fr/hymaia/exo2/clean')
    

    df_population = population_par_departement(df_clean)

    df_population_sorted = df_population.orderBy(col("nb_people").desc(), col("departement").asc())

    df_population_sorted.coalesce(1).write.mode('overwrite').option('header', True).csv('/Users/ayman/vscode/sparkexo0/spark-handson/src/fr/hymaia/exo2/aggregate')



def population_par_departement(df):
    return df.groupBy("departement").agg(count("*").alias("nb_people"))

if __name__ == "__main__":
    main()
