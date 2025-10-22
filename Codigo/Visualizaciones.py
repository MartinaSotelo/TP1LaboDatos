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

carpeta = "C:/Users/perei/Downloads/Datos_para_el_TP/"

# EstEducativos = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EstProductivos = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
# PoblacionEdad= pd.read_csv(carpeta+"PadronPoblacionLimpio.csv")
Consulta1 = pd.read_csv(carpeta+"Consulta01.csv")
Consulta2 = pd.read_csv(carpeta+"Consulta02.csv")
Consulta3= pd.read_csv(carpeta+"Consulta03.csv")
Consulta4= pd.read_csv(carpeta+"Consulta04.csv")
from TablasRelacional import (Departamento,
                              Provincia, 
                              ActividadProductiva, 
                              EstablecimientoEducativo, 
                              NivelEducativo, 
                              RangoEdades, 
                              Departamento_RangoEdades, 
                              Departamento_Provincia, 
                              EstablecimientoEducativo_NivelEducativo, 
                              RangoEdades_NivelEducativo, 
                              ActividadProductiva_Departamento, 
                              Departamento_EstablecimientoEducativo)
from Consulta_1 import ProvinciaYDepartamento
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


#%%

consulta = """
           SELECT provincia
           FROM Provincia
           ORDER BY provincia ASC
           """
a = dd.query(consulta).df()

t = []
i = 0
while i < 24:
    t.append(a.iloc[i,0])
    i += 1
print(t)

r = []
for prov in t:
    n = Consulta3[Consulta3["provincia"] ==prov]["Cantidad de EE"]
    r.append(n)
plt.figure(figsize=(20,15))
plt.boxplot(r, labels=t)
plt.xticks(rotation = 70)
plt.show()


#%%

consulta = """
           SELECT Departamento_id, SUM(Empleados) AS Empleados
           FROM ActividadProductiva_Departamento
           GROUP BY Departamento_id
           """
V4_1 = dd.query(consulta).df()

consulta = """
           SELECT C.Departamento, P.Departamento_id, C.provincia, C.poblacion, C."Cantidad de EE", V.Empleados
           FROM Consulta3 AS C
           INNER JOIN ProvinciaYDepartamento AS P ON P.Departamento = C.Departamento AND P.provincia = C.provincia
           INNER JOIN V4_1 AS V ON P.Departamento_id = V.Departamento_id
           """
V4_2 = dd.query(consulta).df()


plt.figure(figsize=(10,6))
plt.scatter(V4_2["Cantidad de EE"]/V4_2["Poblacion"]*1000, V4_2["Empleados"]/V4_2["Poblacion"]*1000)
plt.xlabel("EE cada 1000 habitantes")
plt.ylabel("Empleados cada 1000 habitantes")
plt.title("Relación entre empleados y EE por departamento")
    

#%%
consulta = """
           SELECT clae6, SUM(Empleo) AS Empleados
           FROM EstProductivos
           GROUP BY clae6
           """
Emp_por_clae = dd.query(consulta).df()
#%%
consulta = """
           SELECT clae6, SUM(CASE WHEN genero = 'Mujeres' THEN Empleo ELSE 0 END) AS Empleadas, SUM(Empleo) AS Empleados_totales
           FROM ActividadProductiva_Departamento
           GROUP BY clae6
           """
Empleados = dd.query(consulta).df()

Empleados["Proporcion_mujeres"] = Empleados["Empleadas"].astype(float)/Empleados["Empleados_totales"].astype(float)*100
#%%

ActividadProductiva_Departamento["Proporcion_mujeres"] = ActividadProductiva_Departamento["EmpleadasMujeres"].astype(float)/ActividadProductiva_Departamento["Empleados"].astype(float)*100   

consulta = """
           SELECT *
           FROM ActividadProductiva_Departamento
           ORDER BY Proporcion_mujeres ASC
           LIMIT 5
           """
mas_prop = dd.query(consulta).df()

consulta = """
           SELECT *
           FROM ActividadProductiva_Departamento
           ORDER BY Proporcion_mujeres DESC
           LIMIT 5
           """
           
menor_prop = dd.query(consulta).df()

consulta = """
           SELECT *
           FROM mas_prop
           UNION ALL
           SELECT *
           FROM menor_prop
           """
V5 = dd.query(consulta).df()


#%%

total_mujeres = EstProductivos.loc[(EstProductivos["genero"] == "Mujeres") & (EstProductivos["clae6"] == 960202), "Empleo"].sum() 
total_varones = EstProductivos.loc[(EstProductivos["genero"] == "Varones") & (EstProductivos["clae6"] == 960202), "Empleo"].sum()
total = total_mujeres + total_varones

porc_mujeres = total_mujeres / total * 100
porc_varones = total_varones / total * 100

print(f"Mujeres: {total_mujeres} ({porc_mujeres:.2f}%)")
print(f"Varones: {total_varones} ({porc_varones:.2f}%)")
