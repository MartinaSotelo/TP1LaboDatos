#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 19 03:16:30 2025

@author: martina
"""

import pandas as pd
import duckdb as dd
import re
import numpy as np

carpeta = "/home/martina/Escritorio/tpLabo1/"

EE_Limpio = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EP_Limpio = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
actividades = pd.read_csv(carpeta+"actividades_establecimientos.csv")
padron_poblacion_new = pd.read_csv(carpeta+"PadronPoblacionLimpio.csv")

from TablasRelacional import Departamento, Provincia, ActividadProductiva, EstablecimientoEducativo, NivelEducativo, RangoEdades, Departamento_RangoEdades, Departamento_Provincia, EstablecimientoEducativo_NivelEducativo, RangoEdades_NivelEducativo, ActividadProductiva_Departamento , Departamento_EstablecimientoEducativo
from Consulta_1 import ProvinciaYDepartamento
#%%

'''Para cada departamento informar la provincia, el nombre del 
departamento y la cantidad de empleados totales en ese departamento, 
para el año 2022. El orden del reporte debe ser alfabético por provincia 
y, dentro de las provincias, descendente por cantidad de empleados.'''

consulta = """ 
            SELECT Provincia, Departamento, SUM(Empleados) AS EmpleadosTotales
            FROM ActividadProductiva_Departamento AS AD
            INNER JOIN
            ProvinciaYDepartamento AS PD
            ON PD.Departamento_id=AD.Departamento_id
            
            GROUP BY Departamento, Provincia
            ORDER BY Provincia, EmpleadosTotales DESC
            """
Departamento_Provincia_Empleados=dd.query(consulta).df()

print('Consulta 2:')
print(Departamento_Provincia_Empleados)