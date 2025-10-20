#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 02:56:50 2025

@author: martina
"""
import pandas as pd
import duckdb as dd

carpeta = "/home/martina/Escritorio/tpLabo1/"

from TablasRelacional import (Departamento,
                              Provincia, 
                              ActividadProductiva, 
                              EstablecimientoEducativo, 
                              NivelEducativo, 
                              RangoEdades, 
                              Departamento_RangoEdades, 
                              Departamento_Provincia, 
                              EstablecimientoEducativo_NivelEducativo, 
                              RangoEdades_NivelEducativo, 
                              ActividadProductiva_Departamento, 
                              Departamento_EstablecimientoEducativo)


from Consulta_1 import ProvinciaYDepartamento

#%%===========================================================================
#                            CONSULTA 4
#=============================================================================
'''Según los datos de 2022, para cada departamento que tenga una cantidad
de empleados mayor que el promedio de los puestos de trabajo de los
departamentos de la misma provincia, indicar: provincia, nombre del
departamento, los primeros tres dígitos del CLAE6 que más empleos genera,
(si no tiene 6 dígitos, agregar un 0 a la izquierda) y la cantidad de empleos en
ese rubro.'''

#Departamento y clae6 cuya cant de empleados es mayor al promedio de empleados por puestos de trabajo
consulta= ''' 
              SELECT Departamento_id, Clae6, Empleados 
              FROM ActividadProductiva_Departamento AS A1
              WHERE Empleados>(
                    SELECT AVG(Empleados)
                    FROM ActividadProductiva_Departamento AS A2
                    WHERE A1.Clae6=A2.Clae6 )   
          '''

MayorQuePromedio=dd.query(consulta).df()

#Le agrego nombre departamento y provincia
consulta= ''' 
              SELECT Departamento, Provincia, 
              SUBSTRING(RIGHT('000000' || clae6, 6), 1, 3) AS Clae3, Empleados
              FROM MayorQuePromedio AS P
              INNER JOIN
              ProvinciaYDepartamento AS PYD
              ON P.Departamento_id=PYD.Departamento_id
          '''

Consulta4=dd.query(consulta).df()