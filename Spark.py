from pyspark.sql import SparkSession


def crear_sesion_spark():
    NUMERO_MAXIMO_CPUS = 3
    SPARK_MASTER = "local[" + str(NUMERO_MAXIMO_CPUS) + "]"

    return SparkSession \
        .builder \
        .master(SPARK_MASTER) \
        .appName("ContarPalabras") \
        .getOrCreate()
