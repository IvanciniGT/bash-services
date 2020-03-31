from unittest import TestCase

from ContadorPalabras import leer_fichero, partir_palabras, limpiar_acentos


class Test(TestCase):

    def test_contar_palabras(self):
        texto = leer_fichero('texto_prueba.txt')
        numero_palabras=len(partir_palabras(texto))
        self.assertEqual(numero_palabras, 14)


    def test_quitar_acentos(self):
        texto = 'Hóla añigo'
        self.assertEqual(limpiar_acentos(texto),'Hola anigo')
