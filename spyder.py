#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  9 09:48:09 2025

@author: Estudiante
"""

import pandas as pd
import duckdb as dd
import re
#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "/home/Estudiante/Descargas/tp labodatos/"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2,3,4], names=["Edad", "Casos", "%", "Acumulado %"], skiprows=12)
padron_poblacion = padron_poblacion.iloc[:-3]
#%%

consulta = """
               SELECT DISTINCT provincia_id, provincia
               FROM dep_ac_sex;
        """

info_provincia = dd.query(consulta).df() #id de provincia -> nombre de provincia
#%%

consulta = """
               SELECT DISTINCT in_departamento, departamento, provincia_id
               FROM dep_ac_sex;
        """

info_depto = dd.query(consulta).df() #id de departamento, nombre de departamento y provincia a la que pertenece
print(info_depto)
#%%

consulta = """
               SELECT *
               FROM padron_poblacion
               WHERE Edad IS NOT NULL
        """

padron_poblacion = dd.query(consulta).df() #sacamos las filas null
# Ahora separamos en distintos dataframes según el departamento
areas = []

for i, row in padron_poblacion.iterrows():
    val = str(row[0])
    match = re.match(r'^AREA\s+#\s*(\d+)', val)
    if match:
        numero_area = int(match.group(1))  # Devuelve el número ***** y lo pasa a int
        areas.append((i, numero_area))


areas.append((len(padron_poblacion), None)) #Acá termina el último dataframe

# En este diccionario vamos guardando el dataframe de cada área
padrones = {}

for i in range(len(areas) - 1):
    fila_inicio = areas[i][0]
    area_id = areas[i][1]
    fila_final = areas[i + 1][0]

    # Con la línea inicial y la final, tomamos cada dataframe
    df_actual = padron_poblacion.iloc[fila_inicio + 2 : fila_final].reset_index(drop=True)
    df_actual.columns = ['Edad', 'Personas', '%', 'Acumulado %']  # Nombramos las columnas
    df_actual['%'] = pd.to_numeric(df_actual['%'], errors = 'coerce') * 100
    df_actual['Acumulado %'] = pd.to_numeric(df_actual['Acumulado %'], errors = 'coerce') * 100
    # Guardar el DataFrame con el nombre requerido
    nombre_df = f"{area_id}"
    padrones[nombre_df] = df_actual

print(padrones["2007"])






















