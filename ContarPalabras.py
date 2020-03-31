from operator import add

import pandas as pandas
from pyspark.sql import SparkSession

from ContadorPalabras import leer_fichero, partir_palabras, limpiar_acentos, agrupar_palabras, borrar_stop_words

texto = leer_fichero('texto.txt')
texto=limpiar_acentos(texto)
texto=texto.upper()
palabras = partir_palabras(texto)
palabras=borrar_stop_words(palabras)
cuenta_de_palabras=agrupar_palabras(palabras)
print(cuenta_de_palabras)

texto = leer_fichero('texto.txt')



######### Lo mismo con Spark

# Session

NUMERO_MAXIMO_CPUS = 1
SPARK_MASTER = "local["+str(NUMERO_MAXIMO_CPUS)+"]"

spark = SparkSession\
            .builder\
            .master(SPARK_MASTER)\
            .appName("ContarPalabras")\
            .getOrCreate()

texto = [ leer_fichero('texto.txt') ]
rdd_1_fila_con_todo_el_texto=spark.sparkContext.parallelize(texto)
rdd_1_fila_por_palabra                             =     rdd_1_fila_con_todo_el_texto.flatMap(partir_palabras)
rdd_1_fila_por_palabra_sin_acentos                 =     rdd_1_fila_por_palabra.map(limpiar_acentos)
rdd_1_fila_por_palabra_sin_acentos_en_mayusulas    =     rdd_1_fila_por_palabra_sin_acentos.map(lambda palabra:palabra.upper())

rdd_limpito = rdd_1_fila_por_palabra_sin_acentos_en_mayusulas.filter(borrar_stop_words)

rdd_con_unos= rdd_limpito.map(lambda palabra: (palabra, 1))
rdd = rdd_con_unos.reduceByKey(add)

lista_puntuada=rdd.collect()

for (palabra, ocurrencias) in lista_puntuada:
    print(palabra+"   "+str(ocurrencias))

spark.stop()