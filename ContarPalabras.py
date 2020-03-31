import pandas as pandas

from ContadorPalabras import leer_fichero, partir_palabras, limpiar_acentos

texto = leer_fichero('texto.txt')
texto=limpiar_acentos(texto)
texto=texto.upper()
palabras = partir_palabras(texto)
print(palabras)
print(len(palabras))
