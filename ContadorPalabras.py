from operator import add

import pandas
import unidecode
import re


# Cuenta las palabras de un texto
#
from pyspark.sql import SparkSession

from Spark import crear_sesion_spark


def partir_palabras(texto):
    palabras = re.split(
        "[.]|" +
        "[,]|" +
        "[:]|" +
        "[;]|" +
        "[-]|" +
        "[–]|" +
        "[_]|" +
        "[{]|" +
        "[}]|" +
        "[']|" +
        "[¡]|" +
        "[!]|" +
        "[¿]|" +
        "[?]|" +
        "[(]|" +
        "[)]|" +
        "[>]|" +
        "[<]|" +
        "[#]|" +
        "[&]|" +
        "[/]|" +
        "[\]]|" +
        "[\[]|" +
        "[“]|" +
        "[\"]|" +
        "[”]" +
        "[”]" +
        "[0-9]+|" +
        "\s", texto)
    palabras = [palabra for palabra in palabras if len(palabra) > 0]
    return palabras


#
# lee un fichero de texto
#
def leer_fichero(fichero):
    #    fichero=open(fichero, 'r')
    #    texto=fichero.read()
    #    fichero.close()
    #    return texto
    with open(fichero, 'r') as fichero:
        return fichero.read()


def limpiar_acentos(texto):
    return unidecode.unidecode(texto)


PALABRAS_VACIAS = None


def borrar_stop_words(palabras):
    global PALABRAS_VACIAS
    if PALABRAS_VACIAS is None:
        PALABRAS_VACIAS = partir_palabras(limpiar_acentos(leer_fichero('stop_words.txt').upper()))
    return list(filter(lambda palabra: palabra not in PALABRAS_VACIAS, palabras))
    #    return list(filter(filtrador, palabras))




def borrar_stop_word(palabra):
    global PALABRAS_VACIAS
    if PALABRAS_VACIAS is None:
        PALABRAS_VACIAS = partir_palabras(limpiar_acentos(leer_fichero('stop_words.txt').upper()))
    return palabra not in PALABRAS_VACIAS


def contar_palabras_unicas(texto):
    texto = limpiar_acentos(texto)
    texto = texto.upper()
    palabras = partir_palabras(texto)
    palabras = borrar_stop_words(palabras)
    cuenta_de_palabras = map(lambda palabra: (palabra, 1), palabras)
    cuenta_de_palabras = pandas.DataFrame(cuenta_de_palabras)
    cuenta_de_palabras = cuenta_de_palabras.groupby(0).sum()
    cuenta_de_palabras = cuenta_de_palabras.sort_values(1, ascending=False)
    cuenta_de_palabras = cuenta_de_palabras.reset_index().values.tolist()
    return cuenta_de_palabras


def contar_palabras_unicas_spark(texto):
    spark = crear_sesion_spark()

    #  Por orden:
    #   - Convertimos el texto e un RDD
    #   - Pasamos de un texto a un listado de palabras
    #   - Les quitamos los acentos
    #   - Las ponemos en mayúsculas
    #   - Convertimos cada palabra en una tupla, añadiendole un 1
    #   - Juntamos las ocurrencias de la misma palabra, sumando los 1s
    #   - Invertimos... ponemos primero el numero y luego la palabra
    #   - Ordenamos por numeros
    #   - Invertimos de nuevo, dejando primero la palabra
    #   - Listo!

    ocurrencias = spark.sparkContext.parallelize(texto) \
        .flatMap(partir_palabras) \
        .map(limpiar_acentos) \
        .map(lambda palabra: palabra.upper()) \
        .filter(lambda palabra: borrar_stop_word(palabra)) \
        .map(lambda palabra: (palabra, 1)) \
        .reduceByKey(add) \
        .map(lambda tupla: (tupla[1], tupla[0])) \
        .sortByKey(False) \
        .map(lambda tupla: (tupla[1], tupla[0])) \
        .collect()
    spark.stop()

    return ocurrencias
