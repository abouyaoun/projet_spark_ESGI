import time
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq


spark = SparkSession.builder.appName("exo4").master("local[*]").config('udfs.jars', 'src/resources/exo4/udf.jar').getOrCreate()


def main():

    data_csv = "/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo4/sell.csv"
    df = spark.read.csv(data_csv, header=True, inferSchema=True)

# Mesurer le temps d'exécution
    start_time = time.time()  # La variable start_time est définie ici

    def col_category_name(category):
        if category < 6:
            return "food"
        else:
            return "furniture"

    udf_col_category_name = udf(lambda category: col_category_name(category), StringType())
    df_with_category_name = df.withColumn("category_name", udf_col_category_name(col("category")))
    df_with_category_name.show(truncate=False)

    end_time = time.time()  # La variable end_time est définie ici
    print(f"Temps d'exécution : {end_time - start_time:.2f} secondes")

    
if __name__ == "__main__":
    main()
