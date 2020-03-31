from random import random as rnd


#
# Estimador de PI
# Calcula PI realizando un número determinado de
#
def estimar_pi(numero_calculos):
    dentro_del_circulo = 0
    for i in range(0, numero_calculos):
        x = rnd()
        y = rnd()
        distancia_al_centro = x * x + y * y
        if distancia_al_centro <= 1:
            dentro_del_circulo += 1
    return (dentro_del_circulo / numero_calculos) * 4
