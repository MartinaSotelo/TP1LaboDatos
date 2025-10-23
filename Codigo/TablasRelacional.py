#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 18 16:21:05 2025

@author: martina
"""
#============================================

"""
Grupo Import_milanesas
Integrantes: 
Dulio Joaquin, 
Risuleo Franco, 
Perez Sotelo Martina

En este archivo armamos todas las tablas del modelo relacional
a partir de la limpieza que le hicimos a las tablas originales en LimpiezaDatasets
"""
#============================================
#Importamos librerias
#============================================
import pandas as pd
import duckdb as dd
import re
import numpy as np
import os
#============================================
#Importamos las tablas limpias y normalizadas
#============================================
carpeta = "~/import_milanesas/TablasLimpias/"
EstEducativos = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EstProductivos = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
PoblacionEdad= pd.read_csv(carpeta+"PadronPoblacionLimpio.csv")

#%%
# casteo todo los ids como varchar por las dudas. 

#============================================
#         TABLAS DE LAS RELACIONES
#============================================
#             PROVINCIA
#============================================
consulta = """
               SELECT DISTINCT CAST(provincia_id AS VARCHAR) as Provincia_id, provincia
               FROM EstProductivos
        """
Provincia = dd.query(consulta).df()

#============================================
#         DEPARTAMENTO
#===========================================
consulta = """
               SELECT DISTINCT CAST(Departamento_id AS VARCHAR) AS Departamento_id, departamento AS Departamento
               FROM EstProductivos
        """

Departamento = dd.query(consulta).df()

#============================================
#        ESTABLECIMIENTO EDUCATIVO
#============================================
consulta = """
               SELECT CAST(Cueanexo AS VARCHAR) AS Cue
               FROM EstEducativos
           """

EstablecimientoEducativo = dd.query(consulta).df()

#============================================
#        NIVEL EDUCATIVO
#============================================

dd.query("""
    DROP TABLE IF EXISTS NivelEducativo;
    CREATE TABLE NivelEducativo (
        NivelEducativo VARCHAR(100) PRIMARY KEY
    );
""")

dd.query("""
    INSERT INTO NivelEducativo (NivelEducativo)
    VALUES ('Jardin'), ('Primario'), ('Secundario'), ('SNU');
""")

NivelEducativo = dd.query("SELECT * FROM NivelEducativo").df()

#============================================
#        ACTIVIDAD PRODUCTIVA
#============================================
consulta = """
               SELECT DISTINCT CAST(clae6 AS VARCHAR) AS Clae6, clae6_desc AS Descripcion
               FROM EstProductivos
           """

ActividadProductiva = dd.query(consulta).df()

#============================================
#        RANGO EDADES
#============================================

dd.query("""
    DROP TABLE IF EXISTS RangoEdades;
    CREATE TABLE RangoEdades (
        Rango VARCHAR(100) PRIMARY KEY
    );
""")

dd.query("""
    INSERT INTO RangoEdades (Rango)
    VALUES ('rango_0_5'), ('rango_6_12'), ('rango_13_17'), ('mayores_18');
""")

RangoEdades = dd.query("SELECT * FROM RangoEdades").df()

#%%
#============================================
#        TABLAS DE LAS RELACIONES
#============================================
#        DEPARTAMENTO-PROVINCIA
#============================================
consulta = """
               SELECT DISTINCT CAST(Departamento_id AS VARCHAR) AS Departamento_id, CAST(provincia_id AS VARCHAR) AS Provincia_id
               FROM EstProductivos
        """

Departamento_Provincia = dd.query(consulta).df()


#============================================
#    DEPARTAMENTO - RANGO EDADES        
#============================================
consulta = """
               SELECT Departamento_id, rango_0_5 AS Cantidad_Habitantes, 'rango_0_5' AS RangoEdad
               FROM PoblacionEdad
               UNION ALL
               SELECT Departamento_id, rango_6_12 AS Cantidad_Habitantes, 'rango_6_12' AS RangoEdad
               FROM PoblacionEdad
               UNION ALL
               SELECT Departamento_id, rango_13_17 AS Cantidad_Habitantes, 'rango_13_17' AS RangoEdad
               FROM PoblacionEdad
               UNION ALL
               SELECT Departamento_id, mayores_18 AS Cantidad_Habitantes, 'mayores_18' AS RangoEdad
               FROM PoblacionEdad
               ORDER BY Departamento_id, RangoEdad
               
        """

Departamento_RangoEdades = dd.query(consulta).df()
#============================================
# ESTABLECIMIENTO EDUCATIVO - NIVEL EDUCATIVO        
#============================================

consulta = """
               SELECT Cueanexo AS Cue, Primario, Jardin, Secundario, SNU
               FROM EstEducativos
        """

EstablecimientoEducativo_NivelEducativo = dd.query(consulta).df()

#Pongo Nivel Educativo como una sola columna
consulta = """
               SELECT Cue, 'Jardin' AS 'NivelEducativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE Jardin = 1
               
               UNION ALL
               
               SELECT Cue, 'Primario' AS 'NivelEducativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE primario = 1
               
               UNION ALL
               
               SELECT Cue, 'Secundario' AS 'NivelEducativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE Secundario = 1
               
               UNION ALL
               
               SELECT Cue, 'SNU' AS 'NivelEducativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE SNU = 1
               
               ORDER BY Cue
        """

EstablecimientoEducativo_NivelEducativo = dd.query(consulta).df()

#============================================
#     RANGO EDADES-NIVEL EDUCATIVO 
#============================================

#armo la tabla desde un diccionario
edad_nivelEducativo = {'RangoEdad': ['rango_0_5','rango_6_12','rango_13_17','mayores_18'],
                       'NivelEducativo':['Jardin', 'Primario','Secundario','SNU']}

RangoEdades_NivelEducativo = pd.DataFrame(edad_nivelEducativo)

#============================================
#    ACTIVIDAD PRODUCTIVA-DEPARTAMENTO  
#============================================

consulta = """
               SELECT CAST(clae6 AS VARCHAR) AS Clae6, CAST(Departamento_id AS VARCHAR) AS Departamento_id,
                   SUM(Empleo) AS Empleados,
                   SUM(CASE WHEN genero = 'Mujeres' THEN empleo ELSE 0 END) AS EmpleadasMujeres,
                   SUM(CASE WHEN genero = 'Mujeres' THEN Empresas_exportadoras ELSE 0 END) AS Empresas_exportadoras_mujeres
               FROM EstProductivos
               GROUP BY Clae6, Departamento_id
               ORDER BY Departamento_id, Clae6
        """

ActividadProductiva_Departamento = dd.query(consulta).df()

#====================================================
#  DEPARTAMENTO-ESTABLECIMIENTO EDUCATIVO  
#====================================================
consulta = """
           SELECT E.Departamento_id, E.Cueanexo AS Cue
           FROM EstEducativos AS E
           
           """

Departamento_EstablecimientoEducativo = dd.query(consulta).df()

# %%


#=============================================================================
#Exporto todas las tablas a csv
#=============================================================================
#Creo la carpeta TablasModelo si no existe
os.makedirs("TablasModelo", exist_ok=True)

#exporto los csv:   

Departamento_EstablecimientoEducativo.to_csv("TablasModelo/Departamento_EstablecimientoEducativo.csv", index=False)
ActividadProductiva_Departamento.to_csv("TablasModelo/ActividadProductiva_Departamento.csv",index=False)
RangoEdades_NivelEducativo.to_csv("TablasModelo/RangoEdades_NivelEducativo.csv",index=False)
EstablecimientoEducativo_NivelEducativo.to_csv("TablasModelo/EstablecimientoEducativo_NivelEducativo.csv",index=False)
Departamento_RangoEdades.to_csv("TablasModelo/Departamento_RangoEdades.csv",index=False)
Departamento_Provincia.to_csv("TablasModelo/Departamento_Provincia.csv",index=False)
RangoEdades.to_csv("TablasModelo/RangoEdades.csv",index=False)
ActividadProductiva.to_csv("TablasModelo/ActividadProductiva.csv",index=False)
NivelEducativo.to_csv("TablasModelo/NivelEducativo.csv",index=False)
EstablecimientoEducativo.to_csv("TablasModelo/EstablecimientoEducativo.csv",index=False)
Departamento.to_csv("TablasModelo/Departamento.csv",index=False)
Provincia.to_csv("TablasModelo/Provincia.csv",index=False)









