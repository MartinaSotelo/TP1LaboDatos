#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 18 16:21:05 2025

@author: martina
"""
import pandas as pd
import duckdb as dd
import re
import numpy as np

#%%
carpeta = "C:/Users/perei/Downloads/Datos_para_el_TP/"
EstEducativos = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EstProductivos = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
PoblacionEdad= pd.read_csv(carpeta+"PadronPoblacionLimpio.csv")

#%%
'''
PROVINCIA (Provincia_id, Provincia)
DEPARTAMENTO (Departamento_id, Departamento)
ESTABLECIMIENTO EDUCATIVO (Cue)
NIVEL EDUCATIVO (NivelEducativo)	
ACTIVIDAD PRODUCTIVA (Clae6)
RANGO EDADES (RangoEdad)
'''
# casteo todo los ids como varchar por las dudas. 
#============================================
#         PROVINCIA
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
               SELECT DISTINCT CAST(in_departamentos AS VARCHAR) AS Departamento_id, UPPER(departamento) AS Departamento
               FROM EstProductivos
        """

Departamento = dd.query(consulta).df()

#============================================
#        ESTABLECIMIENTO EDUCATIVO
#============================================
consulta = """
               SELECT CAST(Cue AS VARCHAR) AS Cue
               FROM EstEducativos
           """

EstablecimientoEducativo = dd.query(consulta).df()

#============================================
#        NIVEL EDUCATIVO
#============================================

dd.query("""
    DROP TABLE IF EXISTS NivelEducativo;
    CREATE TABLE NivelEducativo (
        Nivel VARCHAR(100) PRIMARY KEY
    );
""")

dd.query("""
    INSERT INTO NivelEducativo (Nivel)
    VALUES ('Jardin'), ('Primario'), ('Secundario'), ('SNU');
""")

NivelEducativo = dd.query("SELECT * FROM NivelEducativo").df()

#============================================
#        ACTIVIDAD PRODUCTIVA
#============================================
consulta = """
               SELECT DISTINCT CAST(clae6 AS VARCHAR) AS Clae6
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
'''
DEPARTAMENTO-PROVINCIA (Departamento_id, Provincia_id)
DEPARTAMENTO-RANGO_EDADES (Departamento_id, rango_edad, cant_habitantes)
ESTABLECIMIENTO_EDUCATIVO-NIVEL_EDUCATIVO (Cue, nivel_educativo)
RANGO_EDADES-NIVEL_EDUCATIVO (rango_edad, nivel_educativo)	
ACTIVIDAD_PRODUCTIVA-DEPARTAMENTO (Clae6, Departamento_id, empleados, empleadas_mujeres, empresas_exportadoras, empresas_exportadoras_mujeres)
'''
#============================================
#        DEPARTAMENTO-PROVINCIA
#============================================
consulta = """
               SELECT DISTINCT CAST(in_departamentos AS VARCHAR) AS Departamento_id, CAST(provincia_id AS VARCHAR) AS Provincia_id
               FROM EstProductivos
        """

Departamento_Provincia = dd.query(consulta).df()


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
#Junté los técnicos con los normales
consulta = """
               SELECT Cue, Primario, Jardin, Secundario, SNU
               FROM EstEducativos
        """

EstablecimientoEducativo_NivelEducativo = dd.query(consulta).df()

#Puse Nivel Educativo como una sola columna
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

EstablecimientoEducativo_NivelEducativo= dd.query(consulta).df()

#============================================
#     RANGO EDADES-NIVEL EDUCATIVO 
#============================================
#RANGO_EDADES-NIVEL_EDUCATIVO (rango_edad, nivel_educativo)

#armo la tabla desde un diccionario
edad_nivelEducativo = {'RangoEdad': ['rango_0_5','rango_6_12','rango_13_17','mayores_18'],
                       'NivelEducativo':['Jardin', 'Primario','Secundario','SNU']}

RangoEdades_NivelEducativo = pd.DataFrame(edad_nivelEducativo)

#============================================
#   ACTIVIDAD PRODUCTIVA-DEPARTAMENTO  
#============================================
#ACTIVIDAD_PRODUCTIVA-DEPARTAMENTO (Clae6, Departamento_id, empleados, empleadas_mujeres, empresas_exportadoras, empresas_exportadoras_mujeres)

consulta = """
               SELECT CAST(clae6 AS VARCHAR) AS Clae6, CAST(in_departamentos AS VARCHAR) AS Departamento_id, SUM(Empleo) AS Empleados
               FROM EstProductivos
               GROUP BY Clae6, Departamento_id
               ORDER BY Departamento_id, Clae6
        """

ActividadProductiva_Departamento= dd.query(consulta).df()

consulta = """
               SELECT A.Clae6, A.Departamento_id, A.Empleados, E.genero, E.Empleo, E.Empresas_exportadoras
               FROM ActividadProductiva_Departamento AS A
               JOIN EstProductivos AS E
               ON E.clae6=A.Clae6 AND E.in_departamentos=A.Departamento_id, 
        """

ActividadProductiva_Departamento = dd.query(consulta).df()

consulta = """
                SELECT Clae6, Departamento_id, Empleados, Empresas_exportadoras,
                SUM(CASE WHEN genero = 'Mujeres' THEN empleo ELSE 0 END) AS EmpleadasMujeres
                FROM ActividadProductiva_Departamento
                GROUP BY clae6, Departamento_id, Empresas_Exportadoras, empleados;
        """

ActividadProductiva_Departamento= dd.query(consulta).df()

#%%
consulta = """
           SELECT E.Departamento_id, E.Cue
           FROM EstEducativos AS E
           
           """

Departamento_EstablecimientoEducativo= dd.query(consulta).df()










