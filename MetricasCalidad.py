#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 12 11:45:15 2025

@author: martina
"""

import pandas as pd
import duckdb as dd
import re


#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "/home/martina/Escritorio/tpLabo1/"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)


#%%===========================================================================
# ¿cuantos nombres de provicias coinciden entra padron_ee y dep_ac_sex?
#=============================================================================
print("Porcentajes de coincidencia de nombre provincias entre tablas Est. Educativos y Est. Prod.:")

consulta = """
               SELECT DISTINCT Jurisdicción AS Jurisdiccion
               FROM padron_ee 
               ORDER BY jurisdiccion
           """

Jurisdicciones = dd.query(consulta).df()

consulta = """
               SELECT DISTINCT provincia
               FROM dep_ac_sex 
               ORDER BY provincia
           """

provincias = dd.query(consulta).df()


consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM Jurisdicciones AS J
               INNER JOIN provincias AS p
               ON J.Jurisdiccion = P.provincia;
               
           """

CantidadDeCoincidencias = dd.query(consulta).df()
Total_Coincidencias = CantidadDeCoincidencias.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias / 24 * 100),1)
print(f"{Porcentaje}%")

#%%===========================================================================
# ¿si quito mayusculas y tildes cuantas provincias coinciden?
#=============================================================================
print("Porcentajes de coincidencia de nombre provincias entre tablas Est. Educativos y Est. Prod., cuando se quitan las tildes y se pone todo en minusculas:")
consulta = ''' 
              SELECT DISTINCT lower(strip_accents(provincia)) AS provincia
              FROM dep_ac_sex
              ORDER BY provincia
             
           '''
provincias_v2 = dd.query(consulta).df()

consulta = ''' 
              SELECT DISTINCT lower(strip_accents(jurisdicción)) AS jurisdiccion
              FROM padron_ee
              ORDER BY jurisdiccion
             
           '''
jurisdicciones_v2 = dd.query(consulta).df()

consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM jurisdicciones_v2 AS J
               INNER JOIN provincias_v2 AS p
               ON J.Jurisdiccion = P.provincia;
               
           """

CantidadDeCoincidencias_v2 = dd.query(consulta).df()
Total_Coincidencias_v2 = CantidadDeCoincidencias_v2.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_v2 / 24 * 100),1)
print(f"{Porcentaje}%")

#%%===========================================================================
# Cambio 'caba' por 'ciudad de buenos aires'
#=============================================================================
print("Porcentajes de coincidencia de nombre provincias entre tablas Est. Educativos y Est. Prod.,cuando se quitan las tildes, se pone todo en minusculas y se cambia 'caba' por 'ciudad de buenos aires':")
consulta = '''
              SELECT REPLACE (provincia, 'caba', 'ciudad de buenos aires' ) as provincia
              FROM provincias_v2
           '''       
provincias_v3 = dd.query(consulta).df()


consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM jurisdicciones_v2 AS J
               INNER JOIN provincias_v3 AS p
               ON J.Jurisdiccion = P.provincia;
               
           """

CantidadDeCoincidencias_v3 = dd.query(consulta).df()
Total_Coincidencias_v3 = CantidadDeCoincidencias_v3.at[0, 'Total_Coincidencias']

#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_v3 / 24 * 100),1)
print(f"{Porcentaje}%")

#%%===========================================================================
# ¿cuantos nombres de departamento coinciden entre padron_ee y dep_ac_sex?
#=============================================================================
print("Porcentajes de coincidencia de nombre de departamentos entre tablas Est. Educativos y Est. Prod.:")

consulta = """
               SELECT DISTINCT Departamento as departamento
               FROM padron_ee 
               ORDER BY departamento
           """

departamentos_ee = dd.query(consulta).df()

consulta = """
               SELECT DISTINCT departamento
               FROM dep_ac_sex 
               ORDER BY departamento
           """

departamentos_ep = dd.query(consulta).df()


consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM departamentos_ee AS E
               INNER JOIN departamentos_ep AS p
               ON E.departamento = P.departamento;
               
           """

CantidadDeCoincidencias_Departamento = dd.query(consulta).df()
Total_Coincidencias_dpto = CantidadDeCoincidencias_Departamento.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_dpto / 447 * 100),1)
print(f"{Porcentaje}%")
