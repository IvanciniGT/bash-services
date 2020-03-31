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
