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
carpeta = "/"


EstEducativos = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EstProductivos = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
actividades = pd.read_csv(carpeta+"actividades_establecimientos.csv")
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

#============================================
#         PROVINCIA
#============================================
consulta = """
               SELECT DISTINCT provincia, provincia_id
               FROM EstProductivos
        """

Provincia = dd.query(consulta).df()

#============================================
#         DEPARTAMENTO
#============================================
consulta = """
               SELECT DISTINCT in_departamentos AS Departamento_id, UPPER(departamento) AS departamento
               FROM EstProductivos
        """

Departamento = dd.query(consulta).df()

#============================================
#        ESTABLECIMIENTO EDUCATIVO
#============================================
consulta = """
               SELECT Cue
               FROM EstEducativos
           """

EstablecimientoEducativo = dd.query(consulta).df()

#============================================
#        NIVEL EDUCATIVO
#============================================

dd.query("""
    DROP TABLE IF EXISTS nivel_educativo;
    CREATE TABLE nivel_educativo (
        Nivel VARCHAR(100) PRIMARY KEY
    );
""")

dd.query("""
    INSERT INTO nivel_educativo (Nivel)
    VALUES ('Jardin'), ('Primario'), ('Secundario'), ('SNU');
""")

NivelEducativo = dd.query("SELECT * FROM nivel_educativo").df()

#============================================
#        ACTIVIDAD PRODUCTIVA
#============================================
consulta = """
               SELECT DISTINCT clae6
               FROM EstProductivos
           """

ActividadProductiva = dd.query(consulta).df()

#============================================
#        RANGO EDADES
#============================================

dd.query("""
    DROP TABLE IF EXISTS rango_edades;
    CREATE TABLE rango_edades (
        Rango VARCHAR(100) PRIMARY KEY
    );
""")

dd.query("""
    INSERT INTO rango_edades (Rango)
    VALUES ('rango_0_5'), ('rango_6_12'), ('rango_13_17'), ('mayores_18');
""")

RangoEdades = dd.query("SELECT * FROM rango_edades").df()

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
               SELECT DISTINCT in_departamentos AS Departamento_id, provincia_id
               FROM EstProductivos
        """

Departamento_Provincia = dd.query(consulta).df()

#============================================
#         DEPARTAMENTO-RANGO EDADES 
#============================================
consulta = """
               SELECT Departamento_id, rango_0_5, rango_6_12, rango_13_18, mayores_18
               FROM Departamento
               JOIN PoblacionEdad
               ON PoblacionEdad.id_areas = departamento.Departamento_id
        """

Departamento_RangoEdades = dd.query(consulta).df()

consulta = """
               SELECT Departamento_id, rango_0_5 AS Cantidad_Habitantes, 'rango_0_5' AS RangoEdad
               FROM Departamento_RangoEdades
               UNION ALL
               SELECT Departamento_id, rango_6_12 AS Cantidad_Habitantes, 'rango_6_12' AS RangoEdad
               FROM Departamento_RangoEdades
               UNION ALL
               SELECT Departamento_id, rango_13_18 AS Cantidad_Habitantes, 'rango_13_18 ' AS RangoEdad
               FROM Departamento_RangoEdades
               UNION ALL
               SELECT Departamento_id, mayores_18 AS Cantidad_Habitantes, 'mayores_18' AS RangoEdad
               FROM Departamento_RangoEdades
               ORDER BY Departamento_id, RangoEdad
               
        """

Departamento_RangoEdades = dd.query(consulta).df()
#============================================
# ESTABLECIMIENTO EDUCATIVO - NIVEL EDUCATIVO        
#============================================
#Junté los técnicos con los normales
consulta = """
               SELECT Cue, Primario,
                   CASE
                       WHEN Jardin_infantes = 1 OR Jardin_maternal = 1 THEN 1
                       ELSE NULL
                       END AS Jardin,
                   CASE
                       WHEN Secundario = 1 OR Secundario_INET = 1 THEN 1
                       ELSE NULL
                       END AS Secundario,
                   CASE
                       WHEN SNU = 1 OR SNU_INET = 1 THEN 1
                       ELSE NULL
                       END AS SNU
               FROM EstEducativos
        """

EstablecimientoEducativo_NivelEducativo = dd.query(consulta).df()

#Puse Nivel Educativo como una sola columna
consulta = """
               SELECT Cue, 'jardin' AS 'nivel educativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE jardin = 1
               
               UNION ALL
               
               SELECT Cue, 'Primario' AS 'nivel educativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE primario = 1
               
               UNION ALL
               
               SELECT Cue, 'Secundario' AS 'nivel educativo'
               FROM EstablecimientoEducativo_NivelEducativo
               WHERE Secundario = 1
               
               UNION ALL
               
               SELECT Cue, 'SNU' AS 'nivel educativo'
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
               SELECT clae6, in_departamentos, SUM(Empleo) AS empleados
               FROM EstProductivos
               GROUP BY Clae6, in_departamentos
               ORDER BY In_departamentos, clae6
        """

ActividadProductiva_Departamento= dd.query(consulta).df()


consulta = """
               SELECT A.clae6, A.in_departamentos, A.empleados, E.genero, E.empleo, E.empresas_exportadoras
               FROM ActividadProductiva_Departamento AS A
               JOIN EstProductivos AS E
               ON E.clae6=A.Clae6 AND E.in_departamentos=A.in_departamentos,
               
               
        """

ActividadProductiva_Departamento= dd.query(consulta).df()

consulta = """
                SELECT clae6, in_departamentos, empleados, empresas_exportadoras,
                SUM(CASE WHEN genero = 'Mujeres' THEN empleo ELSE 0 END) AS empleadas_mujeres,
                FROM ActividadProductiva_Departamento1
                GROUP BY clae6, in_departamentos, empresas_exportadoras, empleados;
               
               
        """

ActividadProductiva_Departamento= dd.query(consulta).df() Genero = 'Mujeres' THEN Empleo ELSE NULL END AS mujeres_empleadas,
               FROM EstProductivos
        """

actividadproductiva_departamento= dd.query(consulta).df()
