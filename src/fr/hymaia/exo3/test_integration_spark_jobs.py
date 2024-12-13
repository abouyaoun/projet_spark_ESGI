import unittest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.fr.hymaia.exo2.main import main as main_job1
from src.fr.hymaia.exo2.spark_clean_job import main as main_job2  

class SparkJobIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("IntegrationTest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_integration_jobs(self):
        clean_path = '/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo2/data/clean'

        if not os.path.exists(clean_path):
            self.fail(f"Le fichier {clean_path} n'existe pas. Assurez-vous que le premier job Spark a été exécuté correctement.")

        df_clean = self.spark.read.parquet(clean_path)

        self.assertGreater(df_clean.count(), 0)

        expected_schema = StructType([
            StructField("zip", StringType(), True),
            StructField("city", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),  
            StructField("departement", StringType(), True)
        ])

        if df_clean.schema != expected_schema:
            print("\nLe schéma actuel ne correspond pas au schéma attendu. Voici les différences :")
            for expected_field, actual_field in zip(expected_schema.fields, df_clean.schema.fields):
                if expected_field != actual_field:
                    print(f"- Champ attendu : {expected_field}, Champ actuel : {actual_field}")

            self.fail("Le schéma du DataFrame ne correspond pas au schéma attendu. Veuillez mettre à jour le schéma attendu dans le test.")

        main_job2()

        aggregate_path = '/Users/ayman/vscode/sparkexo0/spark-handson/src/resources/exo2/data/aggregate'
        df_aggregate = self.spark.read.option('header', True).csv(aggregate_path)

        self.assertGreater(df_aggregate.count(), 0)

if __name__ == '__main__':
    unittest.main()
