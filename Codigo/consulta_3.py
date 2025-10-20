#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 02:07:37 2025

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
#                            CONSULTA 3
#=============================================================================

'''Para cada departamento, indicar provincia, nombre del departamento,
cantidad de empresas exportadoras que emplean mujeres (en 2022),
cantidad de EE (de modalidad común) y población total. Ordenar por
cantidad de EE descendente, cantidad de empresas exportadoras
descendente, nombre de provincia ascendente y nombre de departamento
ascendente. No omitir departamentos sin EE o exportadoras con empleo
femenino.''' 
# %%
#Cuento por departamento cantidad de empresas exportadoras que emplean mujeres

consulta= ''' 
            SELECT Departamento_id, COUNT(empresas_exportadoras) AS EmpresasExportadoras_EmpleanMujeres
            FROM ActividadProductiva_Departamento
            WHERE EmpleadasMujeres > 0
            GROUP BY Departamento_id
         '''
        
Departamento_Cant_EmpExp_Mujeres= dd.query(consulta).df()
# %%
#Cuento por departamento cantidad de Est Educativos e indico poblacion total
consulta= ''' 
            SELECT Departamento_id, COUNT(Cue) AS "Cantidad de EE"
            FROM Departamento_EstablecimientoEducativo
            GROUP BY Departamento_id
            
         '''
        
Departamento_Cant_EE= dd.query(consulta).df()
# %%
#Cuento por deparatmento Cantidad de habitantes
consulta= ''' 
            SELECT Departamento_id, SUM(Cantidad_Habitantes) AS Poblacion
            FROM Departamento_RangoEdades
            GROUP BY Departamento_id
            
         '''
        
Departamento_Poblacion= dd.query(consulta).df()

# %%
# hago un join de Departamento_poblacion y Departamento_Cant_EE
consulta= ''' 
            SELECT DP.Departamento_id, Poblacion , "Cantidad de EE"
            FROM Departamento_Poblacion AS DP
            LEFT JOIN 
            Departamento_Cant_EE AS EE
            ON DP.Departamento_id=EE.Departamento_id
            
         '''
        
Dep_poblacion_cantEE= dd.query(consulta).df()
# %%
# hago un join de Dep_poblacion_cantEE y Departamento_Cant_EmpExp_Mujeres
consulta= ''' 
            SELECT PEE.Departamento_id, Poblacion , "Cantidad de EE", EmpresasExportadoras_EmpleanMujeres
            FROM Dep_poblacion_cantEE AS PEE
            LEFT JOIN 
            Departamento_Cant_EmpExp_Mujeres AS XM
            ON XM.Departamento_id=PEE.Departamento_id
            
         '''
        
Dep_poblacion_cantEE_EmpEx_Mujeres= dd.query(consulta).df()

# %%
# tabla anterior le agrego nombre provincia y departamento
consulta= ''' 
            SELECT Departamento, Provincia, Poblacion , "Cantidad de EE", EmpresasExportadoras_EmpleanMujeres
            FROM Dep_poblacion_cantEE_EmpEx_Mujeres AS DCX
            INNER JOIN 
            ProvinciaYDepartamento AS PYD
            ON PYD.Departamento_id=DCX.Departamento_id
            ORDER BY "Cantidad de EE" DESC, EmpresasExportadoras_EmpleanMujeres DESC, Provincia, departamento
            
         '''
        
Consulta3= dd.query(consulta).df()

