#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 12 11:45:15 2025

@author: martina
"""

import pandas as pd
import duckdb as dd

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "/home/martina/Escritorio/tpLabo1/TablasOriginales/"


padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)

#%%===========================================================================
#                               METRICA 1
#%%===========================================================================
# ¿cuantos nombres de provicias coinciden entra padron_ee y dep_ac_sex?
#=============================================================================
print("METRICA 1:")
print("Porcentajes de coincidencia de nombre provincias entre las tablas '2022_padron_oficial_establecimientos_educativos' y 'Datos_por_departamento_actividad_y_sexo' :")

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
#                               METRICA 2    
#%%===========================================================================
# ¿cuantos nombres de departamento coinciden entre padron_ee y dep_ac_sex?
#=============================================================================
print(" ")
print("METRICA 2:")
print("Porcentajes de coincidencia de nombre de departamentos entre las tablas '2022_padron_oficial_establecimientos_educativos' y 'Datos_por_departamento_actividad_y_sexo' :")

consulta = """
               SELECT DISTINCT Departamento as departamento, provincia_normalizado
               FROM padron_ee 
               ORDER BY departamento
           """

departamentos_ee = dd.query(consulta).df()

consulta = """
               SELECT DISTINCT departamento, provincia_normalizado_cambia_CABA AS provincia_normalizado
               FROM dep_ac_sex
               ORDER BY departamento
           """

departamentos_ep = dd.query(consulta).df()


consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM departamentos_ee AS E
               INNER JOIN departamentos_ep AS p
               ON E.departamento = P.departamento AND E.provincia_normalizado = P.provincia_normalizado;
               
           """

CantidadDeCoincidencias_Departamento = dd.query(consulta).df()
Total_Coincidencias_dpto = CantidadDeCoincidencias_Departamento.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_dpto / 528 * 100),1)
print(f"{Porcentaje}%")



#=============================================================================
#METRICA 1
#=============================================================================
print(" ")
print("METRICA 1 ")
print("Porcentaje de consistencia nombre provincia en mis tablas ya limpias:")
consulta = """
               SELECT DISTINCT provincia 
               FROM padron_ee_Limpio
               ORDER BY provincia
           """

P_provincias = dd.query(consulta).df()

consulta = """
               SELECT DISTINCT provincia
               FROM dep_ac_sex_limpio
               ORDER BY provincia
           """

D_provincias = dd.query(consulta).df()
consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM  D_provincias as D
               INNER JOIN P_provincias AS P
               ON D.provincia=P.provincia
               
           """

CantidadDeCoincidencias = dd.query(consulta).df()
Total_Coincidencias = CantidadDeCoincidencias.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias / 24 * 100),1)
print(f"{Porcentaje}%")

#=============================================================================
#METRICA 2
#=============================================================================
print(" ")
print("METRICA 2")
print("Porcentaje de consistencia nombre departamento en mis tablas ya limpias:")
consulta = """
               SELECT DISTINCT departamento, provincia
               FROM padron_ee_Limpio 
               ORDER BY departamento
           """

P_departamentos = dd.query(consulta).df()

consulta = """
               SELECT DISTINCT departamento, provincia
               FROM dep_ac_sex_limpio
               ORDER BY departamento
           """

D_departamentos = dd.query(consulta).df()


consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM D_departamentos AS E
               INNER JOIN P_departamentos AS P
               ON E.departamento = P.departamento AND E.provincia= P.provincia;
               
           """

CantidadDeCoincidencias_Departamento = dd.query(consulta).df()
Total_Coincidencias_dpto = CantidadDeCoincidencias_Departamento.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_dpto / 528 * 100),1)
print(f"{Porcentaje}%")




