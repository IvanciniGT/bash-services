

from ContadorPalabras import leer_fichero, contar_palabras_unicas, contar_palabras_unicas_spark


texto = leer_fichero('texto.txt')
cuenta_de_palabras = contar_palabras_unicas(texto)
print(cuenta_de_palabras)


cuenta_de_palabras = contar_palabras_unicas_spark([texto])
print(cuenta_de_palabras)
