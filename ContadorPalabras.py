import pandas
import unidecode
import re


# Cuenta las palabras de un texto
#
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
        "[”]|" +
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
    with open(fichero,'r') as fichero:
        return fichero.read()


def limpiar_acentos(texto):
    return unidecode.unidecode(texto)


PALABRAS_VACIAS = None


def borrar_stop_words(palabras):
    global PALABRAS_VACIAS
    if PALABRAS_VACIAS == None:
        PALABRAS_VACIAS = partir_palabras(limpiar_acentos(leer_fichero('stop_words.txt').upper()))
    return list(filter( lambda palabra:palabra not in PALABRAS_VACIAS, palabras))
    #    return list(filter(filtrador, palabras))


#def filtrador(palabra):
#    return palabra not in PALABRAS_VACIAS


def agrupar_palabras(palabras):
    cuenta_de_palabras = map(lambda palabra:(palabra, 1),palabras)
    cuenta_de_palabras = pandas.DataFrame(cuenta_de_palabras)
    cuenta_de_palabras = cuenta_de_palabras.groupby(0).sum()
    cuenta_de_palabras = cuenta_de_palabras.sort_values(1, ascending=False)
    return cuenta_de_palabras.reset_index().values.tolist()