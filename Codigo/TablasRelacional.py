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
carpeta = "/home/martina/Escritorio/tpLabo1/"

EstEducativos = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EstProductivos = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
actividades = pd.read_csv(carpeta+"actividades_establecimientos.csv")
PoblacionEdad= pd.read_csv(carpeta+"PadronPoblacionLimpio.csv")

#%%
'''PROVINCIA (Provincia_id, Provincia)

DEPARTAMENTO (Departamento_id, Departamento)
ESTABLECIMIENTO EDUCATIVO (Cue)
NIVEL EDUCATIVO (NivelEducativo)	
ACTIVIDAD PRODUCTIVA (Clae6)'''

#============================================
#        ESTABLECIMIENTO EDUCATIVO
#============================================
consulta = """
               SELECT Cue
               FROM EstEducativos
           """

Establecimiento_Educativo = dd.query(consulta).df()

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
               SELECT DISTINCT UPPER(departamento) AS departamento, iD_departamentos
               FROM EstProductivos
        """

Departamento = dd.query(consulta).df()

#%%

consulta = """
               SELECT DISTINCT in_departamentos,UPPER(departamento) AS departamento, provincia_id, provincia
               FROM EstProductivos
        """

Pertenece_a = dd.query(consulta).df()

#%%

consulta = """
                SELECT Cue
                FROM EstEducativos;
            """

Establecimiento_educativo = dd.query(consulta).df()

#%%


# Crear tabla
dd.query("""
    DROP TABLE IF EXISTS Nivel_educativo;
    CREATE TABLE Nivel_educativo (
        Nivel VARCHAR(100) PRIMARY KEY
    );
""")

# Insertar datos
dd.query("""
    INSERT INTO Nivel_educativo (Nivel)
    VALUES ('Jardin'), ('Primario'), ('Secundario');
""")

# Ver el resultado
Nivel_educativo = dd.query("SELECT * FROM Nivel_educativo").df()

