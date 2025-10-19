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

provincia = dd.query(consulta).df()

#============================================
#         DEPARTAMENTO
#============================================
consulta = """
               SELECT DISTINCT in_departamentos AS Departamento_id, UPPER(departamento) AS departamento
               FROM EstProductivos
        """

departamento = dd.query(consulta).df()

#============================================
#        ESTABLECIMIENTO EDUCATIVO
#============================================
consulta = """
               SELECT Cue
               FROM EstEducativos
           """

establecimiento_educativo = dd.query(consulta).df()

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
    VALUES ('Jardin'), ('Primario'), ('Secundario');
""")

nivel_educativo = dd.query("SELECT * FROM nivel_educativo").df()

#============================================
#        ACTIVIDAD PRODUCTIVA
#============================================
consulta = """
               SELECT DISTINCT clae6
               FROM EstProductivos
           """

actividad_productiva = dd.query(consulta).df()

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

rango_edades = dd.query("SELECT * FROM rango_edades").df()

#%%
'''
DEPARTAMENTO-PROVINCIA (Departamento_id, Provincia_id)
DEPARTAMENTO-RANGO_EDADES (Departamento_id, rango_edad, cant_habitantes)
ESTABLECIMIENTO_EDUCATIVO-NIVEL_EDUCATIVO (Cue, nivel_educativo)
RANGO_EDADES-NIVEL_EDUCATIVO (rango_edad, nivel_educativo)	
ACTIVIDAD_PRODUCTIVA-DEPARTAMENTO (Clae6, Departamento_id, empleados, empleadas_mujeres, empresas_exportadoras, empresas_exportadoras_mujeres)
'''

consulta = """
               SELECT DISTINCT in_departamentos, UPPER(departamento) AS departamento, provincia_id, provincia
               FROM EstProductivos
        """

Pertenece_a = dd.query(consulta).df()


consulta = """
               SELECT Departamento_id, rango_0_5, rango_6_12, rango_13_18, mayores_18
               FROM departamento
               JOIN PoblacionEdad
               ON PoblacionEdad.id_areas = departamento.Departamento_id
        """

departamento_rango_edades = dd.query(consulta).df()

#Junté los técnicos con los normales
consulta = """
               SELECT Cue, Jardin_Infantes, Primario,
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

establecimientoeducativo_niveleducativo = dd.query(consulta).df()



#Lo estaba haciendo y murió chatgpt a la mitad de la explicación
consulta = """
               SELECT clae6, in_departamentos, SUM(Empleo) AS empleados,
                   CASE
                       WHEN Genero = 'Mujeres' THEN Empleo ELSE NULL END AS mujeres_empleadas,
               FROM EstProductivos
        """

actividadproductiva_departamento= dd.query(consulta).df()
