
lista=(1,2,3,4,5,6,7,8,9)

nueva_lista = list(map(lambda numero:numero*2,lista))

print(nueva_lista)


impares= list(  filter( lambda numero:numero % 2 == 1,lista)  )

print(impares)






def saluda(nombre):
    print('Hola '+nombre+' !!!')

variable=saluda
variable('ivan')

variable = lambda nombre:print('Adios '+nombre +'!!!')

variable('ivan')





