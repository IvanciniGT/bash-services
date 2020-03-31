import pandas as pandas

from ContadorPalabras import leer_fichero, partir_palabras, limpiar_acentos, agrupar_palabras, borrar_stop_words

texto = leer_fichero('texto.txt')
texto=limpiar_acentos(texto)
texto=texto.upper()
palabras = partir_palabras(texto)
print(palabras)

palabras=borrar_stop_words(palabras)
print(palabras)

cuenta_de_palabras=agrupar_palabras(palabras)
print(cuenta_de_palabras)
