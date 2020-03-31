import math
from unittest import TestCase

from EstimarPI import estimar_pi
from EstimarPiSpark import estimar_pi_spark


class Test(TestCase):

    NUMERO_CALCULOS = 50 * 1000 * 1000
    NUMERO_BLOQUES = 1

    def test_estimar_pi(self):
        estimacion_de_pi=estimar_pi(self.NUMERO_CALCULOS)
        self.assertAlmostEqual(estimacion_de_pi, math.pi, 1)

    def test_estimar_pi_sprk(self):
        estimacion_de_pi=estimar_pi_spark(self.NUMERO_CALCULOS, self.NUMERO_BLOQUES)
        self.assertAlmostEqual(estimacion_de_pi, math.pi, 1)
