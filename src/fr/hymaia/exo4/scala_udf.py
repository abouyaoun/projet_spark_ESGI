import time
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq


spark = SparkSession.builder.appName("exo4").master("local[*]").config('spark.jars', '/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo4/udf.jar').getOrCreate()



def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))



def main():

    data_csv = "/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo4/sell.csv"
    df = spark.read.csv(data_csv, header=True, inferSchema=True)

        # Mesurer le temps d'exécution
    start_time = time.time()  # La variable start_time est définie ici


    df_with_category_name = df.withColumn("category_name", addCategoryName(col("category")))

    df_with_category_name.show(truncate=False)



    end_time = time.time()  # La variable end_time est définie ici
    print(f"Temps d'exécution : {end_time - start_time:.2f} secondes")

if __name__ == "__main__":
    main()
