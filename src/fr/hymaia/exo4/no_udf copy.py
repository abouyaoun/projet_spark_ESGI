import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Créer une SparkSession
spark = SparkSession.builder \
    .appName("exo4_no_udf") \
    .master("local[*]") \
    .getOrCreate()

def main():
    # Charger les données CSV
    data_csv = "/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo4/sell.csv"
    df = spark.read.csv(data_csv, header=True, inferSchema=True)

    # Mesurer le temps d'exécution
    start_time = time.time()  # La variable start_time est définie ici

    # Utiliser des fonctions Spark natives pour ajouter la colonne "category_name"
    df_with_category_name = df.withColumn(
        "category_name",
        when(col("category") < 6, "food").otherwise("furniture")
    )

    # Afficher les résultats
    df_with_category_name.show(truncate=False)
    

    end_time = time.time()  # La variable end_time est définie ici
    print(f"Temps d'exécution : {end_time - start_time:.2f} secondes")

if __name__ == "__main__":
    main()
