#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 12 11:45:15 2025

@author: martina
"""

import pandas as pd
import duckdb as dd
import re
import numpy as np

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "path"


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
# ¿si quito mayusculas y tildes cuantas provincias coinciden?
#=============================================================================
print(" ")
print("si quito mayusculas y tildes cuantas provincias coinciden?")
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
print(" ")
print("y si, ademas, Cambio 'caba' por 'ciudad de buenos aires'?")
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
#con esto consigo el %100 de coincidencia, entonces:
#%%=========================================================================== 
#                                 LIMPIEZA
#%%===========================================================================
# Hago que coincidan los nombres de provincias en dep_ac_sex y padron_ee
#=============================================================================

consulta = ''' 
             SELECT anio, in_departamentos, departamento, provincia_id, provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
                 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(provincia),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia_normalizado
             FROM dep_ac_sex
           '''
dep_ac_sex = dd.query(consulta).df()

consulta = '''
              SELECT anio, in_departamentos, departamento, provincia_id, provincia_normalizado, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
              REPLACE (provincia_normalizado, 'CABA', 'CIUDAD DE BUENOS AIRES' ) as provincia_normalizado_cambia_CABA
              FROM dep_ac_sex
           '''       
           
dep_ac_sex = dd.query(consulta).df()


consulta = ''' 
              SELECT Jurisdicción, cueanexo, Departamento, "Nivel inicial - Jardín maternal" , "Nivel inicial - Jardín de infantes", Primario, Secundario, SNU, "Secundario - INET", "SNU - INET" ,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Jurisdicción),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia_normalizado
              FROM padron_ee
             
           '''
padron_ee = dd.query(consulta).df()

    
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


#%%===========================================================================
# ¿si quito mayusculas y tildes cuantos departamentos coinciden?
#=============================================================================
print(" ")
print(" ¿si quito mayusculas y tildes cuantos departamentos coinciden?")
consulta = ''' 
              SELECT DISTINCT lower(strip_accents(departamento)) AS departamento, provincia_normalizado
              FROM departamentos_ep
              ORDER BY departamento
             
           '''
departamentos_EP_v2 = dd.query(consulta).df()

consulta = ''' 
              SELECT DISTINCT lower(strip_accents(departamento)) AS departamento, provincia_normalizado
              FROM departamentos_ee
              ORDER BY departamento
             
           '''
departamentos_EE_v2 = dd.query(consulta).df()

consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM departamentos_EP_v2  AS DP
               INNER JOIN departamentos_EE_v2 AS DE
               ON DP.departamento = DE.departamento AND DP.provincia_normalizado = DE.provincia_normalizado;
               
           """

CantidadDeCoincidencias_Dpto_v2 = dd.query(consulta).df()
Total_Coincidencias_Dpto_v2 = CantidadDeCoincidencias_Dpto_v2.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_Dpto_v2 / 528 * 100),1)
print(f"{Porcentaje}%")
#%%===========================================================================
# Me fijo cuales departamentos siguen sin coincidir
#=============================================================================

consulta = """
              SELECT *,
              FROM departamentos_EP_v2  AS DP
              FULL OUTER JOIN
              departamentos_EE_v2 AS DE
              ON DP.departamento = DE.departamento AND DP.provincia_normalizado = DE.provincia_normalizado
              
  
           """

dptosNoCoinciden= dd.query(consulta).df()

consulta = """
              SELECT *,
              FROM dptosNoCoinciden
              WHERE departamento_1 IS NULL OR provincia_normalizado IS NULL
              ORDER BY provincia_normalizado_1, provincia_normalizado
           """

dptosNoCoinciden= dd.query(consulta).df()
#%%===========================================================================
# reemplazo los nombres de departamento para que coincidan
#=============================================================================
print(" ")
print("y si, ademas, reemplazo algunos nombres de departamento?")
consulta = '''
              SELECT *,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE
              (departamento,'coronel de marina l rosales','coronel de marina leonardo rosales' 
              ),'1§ de mayo','1° de mayo'                                                             
              ),'mayor luis j fontana','mayor luis j. fontana'                                                      
              ),'o higgins','ohiggins'                                                       
              ),'doctor manuel belgrano','dr. manuel belgrano'
              ),'general ocampo','general ortiz de ocampo'                                                     
              ), 'coronel felipe varela','general felipe varela'
              ), 'general angel v penaloza','angel vicente penaloza'
              ),'general juan f quiroga','general juan facundo quiroga'
              ),'libertador grl san martin','libertador general san martin'
              ),'general juan martin de pueyrredon', 'juan martin de pueyrredon'
              ), 'antartida argentina', 'antartida argentina' 
              ),'juan b alberdi','juan bautista alberdi'
              ), 'juan f ibarra', 'juan felipe ibarra '
              )AS departamento_limpio 
              FROM departamentos_EE_v2;
              
           '''       

           
departamentos_EE_v3 = dd.query(consulta).df()

consulta = """
               SELECT
               COUNT(*) AS Total_Coincidencias
               FROM departamentos_EE_v3 AS E
               INNER JOIN departamentos_EP_v2 AS p
               ON E.departamento_limpio = P.departamento AND E.provincia_normalizado = P.provincia_normalizado;
               
           """

CantidadDeCoincidencias_Departamento = dd.query(consulta).df()
Total_Coincidencias_dpto = CantidadDeCoincidencias_Departamento.at[0, 'Total_Coincidencias']


#porcentaje de coincidencias 
Porcentaje= round((Total_Coincidencias_dpto / 528 * 100),1)
print(f"{Porcentaje}%")

#%%=========================================================================== 
#                                 LIMPIEZA
#%%===========================================================================
# Hago que coincidan los nombres de departamento en dep_ac_sex y padron_ee
#=============================================================================
consulta = ''' 
             SELECT anio, in_departamentos, departamento, provincia_id, provincia_normalizado_cambia_CABA , clae6, genero, Empleo, establecimientos, empresas_exportadoras,
                 REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(departamento),'á', 'a'),'é', 'e'),'í', 'i'),'ó', 'o'),'ú', 'u') AS departamento_sin_tildes
             FROM dep_ac_sex
           '''
dep_ac_sex = dd.query(consulta).df()

consulta = '''
              SELECT anio, in_departamentos, departamento_sin_tildes, provincia_id, provincia_normalizado_cambia_CABA, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE
              (departamento_sin_tildes,'coronel de marina l rosales','coronel de marina leonardo rosales' 
              ),'1§ de mayo','1° de mayo'                                                             
              ),'mayor luis j fontana','mayor luis j. fontana'                                                      
              ),'o higgins','ohiggins'                                                       
              ),'doctor manuel belgrano','dr. manuel belgrano'
              ),'general ocampo','general ortiz de ocampo'                                                     
              ), 'coronel felipe varela','general felipe varela'
              ), 'general angel v penaloza','angel vicente penaloza'
              ),'general juan f quiroga','general juan facundo quiroga'
              ),'libertador grl san martin','libertador general san martin'
              ),'general juan martin de pueyrredon', 'juan martin de pueyrredon'
              ), 'antartida argentina', 'antartida argentina' 
              ),'juan b alberdi','juan bautista alberdi'
              ), 'juan f ibarra', 'juan felipe ibarra '
              ) as departamento_normalizado
              FROM dep_ac_sex
           '''       
           
dep_ac_sex = dd.query(consulta).df()


consulta = ''' 
              SELECT provincia_normalizado, cueanexo, Departamento, "Nivel inicial - Jardín maternal" , "Nivel inicial - Jardín de infantes", Primario, Secundario, SNU, "Secundario - INET", "SNU - INET" ,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(Departamento),'á', 'a'),'é', 'e'),'í', 'i'),'ó', 'o'),'ú', 'u') AS departamento_normalizado
              FROM padron_ee
             
           '''
padron_ee = dd.query(consulta).df()

#%%===========================================================================
#              LIMPIEZA : termino de limpiar mis tablas para exportar a csv 
#=============================================================================
# PADRON ESTABLECIMIENTOS EDUCATIVOS 
#=============================================================================
#Selecciono las columnas que necesito y renombro
consulta = ''' 
              SELECT provincia_normalizado AS Provincia, cueanexo AS Cue, departamento_normalizado AS departamento, "Nivel inicial - Jardín maternal" AS "Jardin_Maternal", "Nivel inicial - Jardín de infantes" AS "Jardin_Infantes", Primario, Secundario, SNU, "Secundario - INET" AS Secundario_INET, "SNU - INET" AS SNU_INET 
              FROM padron_ee
             
           '''
padron_ee_Limpio = dd.query(consulta).df()

# Elimino todos las filas de colegios que no sean modalidad común (me quedo con jardin, primaria y secundaria)
padron_ee_Limpio.replace(' ', np.nan, inplace=True) #reemplazo espacios en blanco por nulls
padron_ee_Limpio.dropna(thresh=4,inplace=True)

#=============================================================================
# DATOS DEPARTAMENTO POR ACTIVIDAD Y SEXO
#=============================================================================

consulta = ''' 
             SELECT anio, in_departamentos, departamento_normalizado AS departamento, provincia_id, provincia_normalizado_cambia_CABA AS provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
             FROM dep_ac_sex
             WHERE anio = 2022
           '''
dep_ac_sex_limpio = dd.query(consulta).df()

consulta = ''' 
             SELECT  in_departamentos, departamento_normalizado AS departamento, provincia_id, provincia_normalizado_cambia_CABA AS provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
             FROM dep_ac_sex
             
           '''
dep_ac_sex_limpio = dd.query(consulta).df()


#=============================================================================
# Con mis dos metricas anteriores evaluo la calidad de consistencia de mis tablas limpias
#=============================================================================
#METRICA 1
#=============================================================================
print(" ")
print("METRICA 1 ")
print("Porcentaje de consistencia nombre provincia en tablas limpias:")
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
print("Porcentaje de consistencia nombre departamento en tablas limpias:")
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


#Exporto a csv
dep_ac_sex_limpio.to_csv("DepartamentoActivdadySexoLimpio.csv", index=False)
padron_ee_Limpio.to_csv("PadronEstablecimientosEducativosLimpio.csv",index=False)

