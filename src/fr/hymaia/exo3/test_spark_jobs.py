import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from src.fr.hymaia.exo2.main import ageMajeur
from src.fr.hymaia.exo2.spark_clean_job import population_par_departement

class SparkJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ageMajeur_nominal(self):
        data = [("Alice", 23), ("Bob", 17), ("Charlie", 18)]
        df = self.spark.createDataFrame(data, ["name", "age"])
        df_result = ageMajeur(df)

        result_ages = [row['age'] for row in df_result.collect()]
        self.assertEqual(result_ages, [23, 18])

    def test_ageMajeur_erreur_age_manquant(self):
        data = [("Alice", "Paris"), ("Bob", "Marseille")]
        df = self.spark.createDataFrame(data, ["nom", "ville"])
        
        with self.assertRaises(Exception) as context:
            ageMajeur(df)
        
        self.assertTrue("age" in str(context.exception))

    def test_population_par_departement(self):
        data = [("Alice", "75001"), ("Bob", "13002"), ("Charlie", "20100"), ("David", "20200")]
        df = self.spark.createDataFrame(data, ["name", "zip"])
        
        df = df.withColumn("departement", 
                           when((col("zip").substr(1, 5) <= "20190"), "2A")
                           .when((col("zip").substr(1, 5) > "20190") & (col("zip").substr(1, 5) < "21000"), "2B")
                           .otherwise(col("zip").substr(1, 2)))
        
        df_result = population_par_departement(df)

        result = df_result.collect()

        expected_results = {
            "75": 1,
            "13": 1,
            "2A": 1,
            "2B": 1
        }
        
        for row in result:
            self.assertEqual(row["nb_people"], expected_results[row["departement"]])

if __name__ == '__main__':
    unittest.main()