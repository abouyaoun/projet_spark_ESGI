from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum
from pyspark.sql.window import Window

# Créer une SparkSession
spark = SparkSession.builder \
    .appName("exo4_no_udf") \
    .master("local[*]") \
    .getOrCreate()

def main():
    # Charger les données CSV
    data_csv = "/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo4/sell.csv"
    df = spark.read.csv(data_csv, header=True, inferSchema=True)

    # Ajouter la colonne "category_name"
    df_with_category_name = df.withColumn(
        "category_name",
        when(col("category") < 6, "food").otherwise("furniture")
    )

    # Définir une spécification de fenêtre pour la somme par jour et par catégorie
    window_spec_day = Window.partitionBy("category_name", "date")

    # Calculer la somme des prix par jour et par catégorie
    df_with_total_per_day = df_with_category_name.withColumn(
        "total_price_per_category_per_day",
        sum("price").over(window_spec_day)
    )

    # Définir une spécification de fenêtre pour les 30 derniers jours
    window_spec_last_30_days = Window.partitionBy("category_name") \
                                      .orderBy("date") \
                                      .rowsBetween(-30, 0)

    # Calculer la somme des prix sur les 30 derniers jours
    df_with_total_last_30_days = df_with_total_per_day.withColumn(
        "total_price_per_category_per_day_last_30_days",
        sum("price").over(window_spec_last_30_days)
    )

    # Afficher les résultats
    df_with_total_last_30_days.show(200, truncate=False)

if __name__ == "__main__":
    main()
    spark.stop()
