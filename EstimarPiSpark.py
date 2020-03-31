
from pyspark.sql import SparkSession

from EstimarPI import estimar_pi


NUMERO_MAXIMO_CPUS = 3
SPARK_MASTER = "local["+str(NUMERO_MAXIMO_CPUS)+"]"


def estimar_pi_spark(calculos,numero_bloques):

    spark = SparkSession\
            .builder\
            .master(SPARK_MASTER)\
            .appName("EstimarPISpark")\
            .getOrCreate()

    calculos_por_bloque = int(calculos / numero_bloques)
    bloques = [calculos_por_bloque] * numero_bloques

    total = spark.sparkContext.parallelize(bloques, numero_bloques).map(estimar_pi).reduce(sumar)

    return total / numero_bloques


def sumar(a, b):
    return a + b


