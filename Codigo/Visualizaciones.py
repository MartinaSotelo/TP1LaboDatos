#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 21 01:10:28 2025

@author: martina
"""
import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

carpeta = "/home/martina/Escritorio/tpLabo1/"

Consulta1 = pd.read_csv(carpeta+"Consulta01.csv")
Consulta2 = pd.read_csv(carpeta+"Consulta02.csv")
Consulta3= pd.read_csv(carpeta+"Consulta03.csv")
Consulta4= pd.read_csv(carpeta+"Consulta04.csv")

'''
Cantidad de empleados por provincia, para 2022. Mostrarlos ordenados de
manera decreciente por dicha cantidad 
'''

consulta = ''' 
                SELECT Provincia, SUM(EmpleadosTotales) AS 'Cantidad de Empleados'
                FROM Consulta2
                GROUP BY Provincia
                ORDER BY SUM(EmpleadosTotales) DESC

'''

V1= dd.query(consulta).df()



#creo la figura
plt.figure(figsize=(10, 8))

#creo grafico barras horizontal
plt.barh(V1['provincia'], V1['Cantidad de Empleados'], color='skyblue')

#titulo y etiqueta de los ejes
plt.title('Cantidad de Empleados por Provincia (2022)', fontsize=16, pad=10)
plt.xlabel('Cantidad de Empleados', fontsize=12)
plt.ylabel('Provincia', fontsize=12)

#Utilizo formateador de matloplib para poner el valor de Cantidad de empleados.
formatter = ScalarFormatter(useOffset=False, useMathText=True)
plt.gca().xaxis.set_major_formatter(formatter)
plt.ticklabel_format(style='plain', axis='x')


plt.grid(axis='x', linestyle='--') # Agreg0 lineas verticales
plt.tight_layout() #se ajusta el tamaño
plt.show()

''' grafico 2'''
niveles = {
    'Jardines': ('Jardines', 'Poblacion Jardines'),
    'Primarias': ('Primarias', 'Poblacion Primarias'),
    'Secundarias': ('Secundarias', 'Poblacion Secundarias'),
    
}

plt.figure(figsize=(12, 8))

# Iterar sobre cada nivel para generar los puntos y colores
for nivel, (col_ee, col_pob) in niveles.items():
    plt.scatter(
        Consulta1[col_pob],           # Eje X: Población
        Consulta1[col_ee],            # Eje Y: Cantidad de Establecimientos
        label=nivel,           # Nombre para la leyenda
        s=1                # Tamaño de los puntos
    )

# Formatear el eje X para números grandes (como se discutió previamente)
from matplotlib.ticker import FuncFormatter

def formato_miles_y_millones(x, pos):
    if x >= 1e6:
        return f'{x*1e-6:.1f} M'
    elif x >= 1e3:
        return f'{int(x/1000)} K'
    return f'{int(x)}'

formatter = FuncFormatter(formato_miles_y_millones)
plt.gca().xaxis.set_major_formatter(formatter)


# Personalización
plt.title('Establecimientos Educativos vs. Población por Nivel (por Departamento)', fontsize=16, pad=20)
plt.xlabel('Población del Grupo Etario (Formato K/M)', fontsize=12)
plt.ylabel('Cantidad de Establecimientos Educativos (EE)', fontsize=12)
plt.legend(title="Nivel Educativo", loc='upper right')
plt.grid(True, linestyle='--', alpha=0.5)

plt.tight_layout()
plt.show()