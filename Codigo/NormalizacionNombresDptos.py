#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  9 09:48:09 2025

@author: Estudiante
"""

import pandas as pd
import duckdb as dd
import re
import numpy as np
#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "/home/martina/Escritorio/tpLabo1/"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)
DepartamentoNormalizado = pd.read_csv(carpeta +"departamentos.csv")



#%%=========================================================================== 
# Tomo las columnas nombre dpto, nombre provincia y id's correspondientes
# de DepartamentoNormalizado
#===========================================================================
consulta = ''' 
             SELECT id AS Departamento_id, nombre AS departamento, provincia_id, provincia_nombre AS provincia
             FROM DepartamentoNormalizado
           '''
Normalizacion = dd.query(consulta).df()

#%%=============================================================================
# Dep_ac_sex.
#=============================================================================
#en dep_ac_sex hay 527 departamentos  (id)


#corrijo los ids  para que sean iguales al id del normalizado 
#(sino voy a perder estos 3 departamentos en mi limpieza)
dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(6217, 6218)
dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(94007, 94008)
dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(94014, 94015)


# Normalizo nombres de departamento y provincia
consulta = ''' 
             SELECT N.departamento_id, N.departamento, N.provincia_id, N.provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
             FROM dep_ac_sex AS X
             INNER JOIN
             Normalizacion AS N
             ON in_departamentos = N.departamento_id 
             WHERE anio = '2022'
           '''
dep_ac_sex_normalizado = dd.query(consulta).df()

# %%
#=============================================================================
#testeo que no pierdo departamentos (la cantidad  de dptos es la misma en ambas tablas)
#=============================================================================
"""
#en dep_ac_sex hay 527 departamentos (id)
consulta = ''' 
             SELECT in_departamentos
             FROM dep_ac_sex
             GROUP BY in_departamentos
             
           '''
dep_ac_sex_departamentos = dd.query(consulta).df()

#en dep_ac_sex_normalizado hay 527 departamentos (id)
consulta = ''' 
             SELECT departamento_id
             FROM dep_ac_sex_normalizado
             GROUP BY departamento_id
             
           '''
dep_ac_sex_normalizado_departamentos = dd.query(consulta).df()

#La diferencia me da 0 :D bien!!
consulta = ''' 
             SELECT N.departamento_id AS Id_del_Normalizado, X.in_departamentos AS id_del_original
             FROM dep_ac_sex_departamentos AS X
             LEFT JOIN
             dep_ac_sex_normalizado_departamentos AS N
             ON X.in_departamentos = N.departamento_id 
             WHERE N.departamento_id IS NULL
           '''
diferenciaIds = dd.query(consulta).df()"""
#%%=============================================================================
# padron_ee: pongo NULLS donde hay espacios en blanco.
#=============================================================================
padron_ee.replace(' ', np.nan, inplace=True)
#%%===========================================================================
# quiero hacer que coincidan los nombres de departamento en Normalizacion y padron_ee
#=============================================================================
consulta = ''' 
              SELECT Común, cueanexo, Departamento, "Nivel inicial - Jardín maternal" AS JardinMaternal, "Nivel inicial - Jardín de infantes" AS JardinInfantes, Primario, Secundario, SNU, "Secundario - INET" AS SecundarioInet, "SNU - INET" AS SNUInet,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Jurisdicción),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia
              FROM padron_ee
             
           '''
padron_ee_mayus_sinTildes1 = dd.query(consulta).df()

consulta = ''' 
              SELECT común, provincia, cueanexo, Departamento, JardinMaternal , JardinInfantes, Primario, Secundario, SNU, SecundarioInet, SnuInet,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Departamento),'á', 'a'),'é', 'e'),'í', 'i'),'ó', 'o'),'ú', 'u') AS departamento_normalizado
              FROM padron_ee_mayus_sinTildes1
             
           '''
padron_ee_mayus_sinTildes2 = dd.query(consulta).df()

consulta = ''' 
              SELECT departamento_id,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(departamento),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS departamento,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(provincia),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia,
              provincia_id
              FROM Normalizacion
             
           '''
Normalizacion_mayus_sinTildes = dd.query(consulta).df()

consulta = ''' 
             SELECT  P.provincia, P.Departamento, N.departamento, N.provincia
             FROM padron_ee_mayus_sinTildes2 AS P
             
             LEFT JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             WHERE N.departamento IS NULL AND P.Departamento  NOT LIKE 'Comuna%'
             GROUP BY N.departamento, N.provincia, P.provincia, P.Departamento
             
             
           '''
diferencia = dd.query(consulta).df()


dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(6217, 6218)


#CORRIJO LOS NOMBRES QUE DIFIEREN EN PADRON_EE


padron_ee_mayus_sinTildes2['provincia'] = padron_ee_mayus_sinTildes2['provincia'].replace("TIERRA DEL FUEGO","TIERRA DEL FUEGO, ANTARTIDA E ISLAS DEL ATLANTICO SUR")
padron_ee_mayus_sinTildes2['provincia'] = padron_ee_mayus_sinTildes2['provincia'].replace("CIUDAD DE BUENOS AIRES","CIUDAD AUTONOMA DE BUENOS AIRES")

CORRECCIONES_DEPARTAMENTO = {
    
    "GENERAL GUEMES": "GENERAL GÜEMES", 
    "GENERAL JUAN F QUIROGA": "GENERAL JUAN FACUNDO QUIROGA",
    "MALARGUE": "MALARGÜE",
    "LIBERTADOR GRL SAN MARTIN": "LIBERTADOR GENERAL SAN MARTIN", 
    "TOLHUIN": "TOLHUIN",
    "CORONEL DE MARINA L ROSALES": "CORONEL DE MARINA LEONARDO ROSALES",
    "1§ DE MAYO": "1° DE MAYO",
    "1 DE MAYO": "1° DE MAYO",
    "DOCTOR MANUEL BELGRANO": "DR. MANUEL BELGRANO",
    "DR MANUEL BELGRANO": "DR. MANUEL BELGRANO",
    "CORONEL FELIPE VARELA": "GENERAL FELIPE VARELA",
    "GENERAL ANGEL V PEÑALOZA": "ANGEL VICENTE PEÑALOZA" ,
    "GENERAL JUAN MARTIN DE PUEYRREDON": "JUAN MARTIN DE PUEYRREDON",
    "JUAN MARTIN DE PUEYRREDON": "JUAN MARTÍN DE PUEYRREDÓN", 
    "USHUAIA": "USHUAIA",
    "MAYOR LUIS J FONTANA": "MAYOR LUIS J. FONTANA",
    "ANTÁRTIDA ARGENTINA":"ANTARTIDA ARGENTINA",
    "JUAN F IBARRA": "JUAN FELIPE IBARRA",
    "O HIGGINS": "O'HIGGINS",
    "GENERAL OCAMPO": "GENERAL ORTIZ DE OCAMPO",
    "GUER AIKE": "GÜER AIKE",
    "JUAN B ALBERDI": "JUAN BAUTISTA ALBERDI",
    "Comuna 6" :"COMUNA 6",
	"Comuna 1":"COMUNA 1",
	"Comuna 4":"COMUNA 4",
"Comuna 5":"COMUNA 5",
"Comuna 12":"COMUNA 12",
"Comuna 2": "COMUNA 2",
"Comuna 10"	:"COMUNA 10",
	"Comuna 11":"COMUNA 11",
	"Comuna 3": "COMUNA 3",
	"Comuna 2":"COMUNA 2",
"Comuna 8": "COMUNA 8",
	"Comuna 13":"COMUNA 13",
	"Comuna 14":"COMUNA 14",
	"Comuna 9":"COMUNA 9",
	"Comuna 15":"COMUNA 15",
    "Comuna 7":"COMUNA 7"
}


padron_ee_mayus_sinTildes2['Departamento'] = padron_ee_mayus_sinTildes2['Departamento'].replace(CORRECCIONES_DEPARTAMENTO)



#===========================
# TODO ESTO SON CONSULTAS QUE USE PARA AVERIGUAR
#===================
#Aca veo los dptos que difieren
consulta = ''' 
             SELECT  P.provincia, P.Departamento, N.departamento, N.provincia
             FROM padron_ee_mayus_sinTildes2 AS P
             
             LEFT JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             WHERE N.departamento IS NULL AND P.Departamento  NOT LIKE 'Comuna%'
             GROUP BY N.departamento, N.provincia, P.provincia, P.Departamento
             
             
           '''
diferencia2 = dd.query(consulta).df()


#Aca miro que las Comunas difieren y el nombre de Ciudad de Buenos Aires
consulta = ''' 
             SELECT  P.provincia, P.Departamento, N.departamento, N.provincia
             FROM padron_ee_mayus_sinTildes2 AS P
             
             LEFT JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             WHERE N.provincia IS NULL 
             GROUP BY N.departamento, N.provincia, P.provincia, P.Departamento
             
             
           '''
diferencia3 = dd.query(consulta).df()


#Recupero el Id !! :D
consulta = ''' 
             SELECT  P.provincia, P.Departamento, N.departamento, N.provincia
             FROM padron_ee_mayus_sinTildes2 AS P
             
             INNER JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             GROUP BY N.departamento, N.provincia, P.provincia, P.Departamento
             
           '''
join = dd.query(consulta).df()

consulta = ''' 
             SELECT Departamento, Jurisdicción
             FROM padron_ee
             GROUP BY Departamento, Jurisdicción
             
             
           '''
CantDptosPadron_ee = dd.query(consulta).df()

#========================================
# Recupero el Id y normalizo nombres de la tabla
#======================================
consulta = ''' 
             SELECT N.departamento_id As departamento_id, común,  cueanexo, JardinMaternal , JardinInfantes, Primario, Secundario, SNU, SecundarioInet, SnuInet,
             FROM padron_ee_mayus_sinTildes2 AS P
             
             INNER JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             
           '''
Padron_ee_conIDDepto = dd.query(consulta).df()
#======================================
consulta = ''' 
             SELECT N.departamento_id, N.departamento, N.provincia,  común,  cueanexo, JardinMaternal , JardinInfantes, Primario, Secundario, SNU, SecundarioInet, SnuInet,
             FROM Padron_ee_conIDDepto AS P
             
             INNER JOIN
             
             Normalizacion AS N 
             ON N.departamento_id = P.departamento_id
             
           '''
Padron_ee_limpio = dd.query(consulta).df()

