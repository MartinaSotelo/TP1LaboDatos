import pandas as pd
import duckdb as dd
import re
import numpy as np

Carpeta = "/"

from TablasRelacional import Departamento, Provincia, ActividadProductiva, EstablecimientoEducativo, NivelEducativo, RangoEdades, Departamento_RangoEdades, Departamento_Provincia, EstablecimientoEducativo_NivelEducativo, RangoEdades_NivelEducativo, ActividadProductiva_Departamento , Departamento_EstablecimientoEducativo
#%%
#=================================================================
#                         CONSULTA 1
#=================================================================
'''Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de Establecimientos Educativos (EE) de cada nivel educativo,
considerando solamente la modalidad común, y la cantidad de habitantes con
edad correspondiente al nivel educativos listado.El orden del reporte debe 
ser alfabético por provincia y dentro de las provincias descendente por
cantidad de escuelas primarias.'''

consulta = """
           SELECT P.Provincia, P.Provincia_id, D.Departamento_id, D.Departamento
           FROM Provincia AS P
           INNER JOIN
           Departamento_Provincia AS DP
           ON P.Provincia_id = DP.Provincia_id
           INNER JOIN
           Departamento AS D
           ON D.Departamento_id=DP.Departamento_id
           """
            
ProvinciaYDepartamento=dd.query(consulta).df()

consulta = """
           SELECT Cue, RN.NivelEducativo, RangoEdad
           FROM EstablecimientoEducativo_NivelEducativo AS EN
           INNER JOIN
           RangoEdades_NivelEducativo AS RN
           ON RN.NivelEducativo = EN.NivelEducativo
           
           """
            
EdadesNivelYCue=dd.query(consulta).df()

consulta = """
           SELECT PYD.Departamento_id, Cantidad_Habitantes, RangoEdad, Provincia, Departamento
           FROM Departamento_RangoEdades AS DRE
           INNER JOIN
           ProvinciaYDepartamento AS PYD
           ON PYD.Departamento_id=DRE.Departamento_id
           
           """
            
CantHabitantes_Por_Departamento_RangoEdades=dd.query(consulta).df()

consulta = """
           SELECT ENC.Cue, Departamento_id, NivelEducativo, 
           FROM Departamento_EstablecimientoEducativo AS DEE
           INNER JOIN
           EdadesNivelYCue AS ENC
           ON ENC.Cue=DEE.Cue
           
           """
            
Cue_Departamento_NivelEducativo=dd.query(consulta).df()

consulta = """
           SELECT Departamento_id, NivelEducativo,
           COUNT(DISTINCT Cue) AS Cantidad_EstablecimientosEducativos
           FROM Cue_Departamento_NivelEducativo
           GROUP BY Departamento_id, NivelEducativo;
           """
            
CantEE_por_NivelEducativo_por_Departamento=dd.query(consulta).df()

consulta = """
           SELECT CND.Departamento_id, Departamento, Provincia, NivelEducativo, Cantidad_EstablecimientosEducativos
           FROM CantEE_por_NivelEducativo_por_Departamento CND
           INNER JOIN 
           ProvinciaYDepartamento AS PYD
           ON CND.Departamento_id = PYD.Departamento_id
           
           """
            
CantEE_por_NivelEducativo_por_DepartamentoYProvincia=dd.query(consulta).df()

consulta = """
           SELECT Departamento_id, Departamento, Provincia, RNE.NivelEducativo, RangoEdad, Cantidad_EstablecimientosEducativos
           FROM CantEE_por_NivelEducativo_por_DepartamentoYProvincia CNDP
           INNER JOIN 
           RangoEdades_NivelEducativo AS RNE
           ON RNE.NivelEducativo = CNDP.NivelEducativo
           
           """
            
CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia=dd.query(consulta).df()

consulta = """
           SELECT  Departamento, Provincia, NivelEducativo, CE.RangoEdad, Cantidad_EstablecimientosEducativos
           FROM CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia CE
           INNER JOIN 
           Departamento_RangoEdades DR
           ON DR.Departamento_id = CE.Departamento_id AND DR.RangoEdad = CE.RangoEdad
         
           """
            
CantHabitantes_CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia=dd.query(consulta).df()

#Masomenos estaria la tabla pero habria que acomodarla como la del ejemplo :(
