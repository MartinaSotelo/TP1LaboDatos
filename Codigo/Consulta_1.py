import pandas as pd
import duckdb as dd

carpeta = "/"

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

#%%===========================================================================
#                            CONSULTA 1
#=============================================================================
'''Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de Establecimientos Educativos (EE) de cada nivel educativo,
considerando solamente la modalidad común, y la cantidad de habitantes con
edad correspondiente al nivel educativos listado.El orden del reporte debe 
ser alfabético por provincia y dentro de las provincias descendente por
cantidad de escuelas primarias.'''
#=============================================================================
# Junto provincia y departamento
#=============================================================================
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
#=============================================================================
# Junto RangoEdades, Nivel educativo y Cueanexo
#=============================================================================

consulta = """
           SELECT Cue, RN.NivelEducativo, RangoEdad
           FROM EstablecimientoEducativo_NivelEducativo AS EN
           INNER JOIN
           RangoEdades_NivelEducativo AS RN
           ON RN.NivelEducativo = EN.NivelEducativo
           
           """
            
EdadesNivelYCue=dd.query(consulta).df()
#=============================================================================
# saco por cada departamento Cant. de Habitantes por rango de edad
#=============================================================================

consulta = """
           SELECT PYD.Departamento_id, Cantidad_Habitantes, RangoEdad, Provincia, Departamento
           FROM Departamento_RangoEdades AS DRE
           INNER JOIN
           ProvinciaYDepartamento AS PYD
           ON PYD.Departamento_id=DRE.Departamento_id
           
           """
            
CantHabitantes_Por_Departamento_RangoEdades=dd.query(consulta).df()
#=============================================================================
#indico por departamento establecimientos educativos segregado por nivel educativo
#=============================================================================

consulta = """
           SELECT ENC.Cue, Departamento_id, NivelEducativo, 
           FROM Departamento_EstablecimientoEducativo AS DEE
           INNER JOIN
           EdadesNivelYCue AS ENC
           ON ENC.Cue=DEE.Cue
           
           """
            
Cue_Departamento_NivelEducativo=dd.query(consulta).df()
#=============================================================================
#Cuento la cantidad de Establecimientos educativos de tabla anterior
#=============================================================================


consulta = """
           SELECT Departamento_id, NivelEducativo,
           COUNT(Cue) AS Cantidad_EstablecimientosEducativos
           FROM Cue_Departamento_NivelEducativo
           GROUP BY Departamento_id, NivelEducativo;
           """
            
CantEE_por_NivelEducativo_por_Departamento=dd.query(consulta).df()
#=============================================================================
#Le sumo dato de Departamento y Provincia a tabla anterior
#=============================================================================

consulta = """
           SELECT CND.Departamento_id, Departamento, Provincia, NivelEducativo, Cantidad_EstablecimientosEducativos
           FROM CantEE_por_NivelEducativo_por_Departamento AS CND
           INNER JOIN 
           ProvinciaYDepartamento AS PYD
           ON CND.Departamento_id = PYD.Departamento_id
           
           """
            
CantEE_por_NivelEducativo_por_DepartamentoYProvincia=dd.query(consulta).df()
#=============================================================================
# Agrego el rango de edad correspondiente a cada nivel educativo a tabla anterior
#=============================================================================

consulta = """
           SELECT Departamento_id, Departamento, Provincia, RNE.NivelEducativo, RangoEdad, Cantidad_EstablecimientosEducativos
           FROM CantEE_por_NivelEducativo_por_DepartamentoYProvincia AS CNDP
           INNER JOIN 
           RangoEdades_NivelEducativo AS RNE
           ON RNE.NivelEducativo = CNDP.NivelEducativo
           
           """
            
CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia=dd.query(consulta).df()
#=============================================================================
# indico cuantos habitantes hay de cada rango de edad correspondiente a cada nivel educativo
# y agrego a la tabla anterior
#=============================================================================

consulta = """
           SELECT DH.Departamento_id, DH.Departamento, DH.Provincia, NivelEducativo, CE.RangoEdad, Cantidad_EstablecimientosEducativos, Cantidad_Habitantes
           FROM CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia AS CE
           INNER JOIN 
           CantHabitantes_Por_Departamento_RangoEdades AS DH
           ON DH.Departamento_id = CE.Departamento_id AND DH.RangoEdad = CE.RangoEdad;
         
           """
            
CantHabitantes_CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia=dd.query(consulta).df()
#=============================================================================
#Pongo cada nivel educativo como columna con un SUM y CASE
#=============================================================================
consulta = """
               SELECT Departamento_id, Departamento, Provincia,
               SUM(CASE WHEN NivelEducativo = 'Jardin' THEN  Cantidad_EstablecimientosEducativos ELSE 0 END) AS 'Jardines'
               ,SUM(CASE WHEN NivelEducativo = 'Primario' THEN Cantidad_EstablecimientosEducativos ELSE 0 END) AS 'Primarias'
               ,SUM(CASE WHEN NivelEducativo = 'Secundario' THEN Cantidad_EstablecimientosEducativos ELSE 0 END) AS 'Secundarias'
              , SUM(CASE WHEN NivelEducativo = 'SNU' THEN Cantidad_EstablecimientosEducativos ELSE 0 END) AS 'Superiores No Universitarios'
               FROM CantHabitantes_CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia AS C
               GROUP BY Departamento_id, Departamento, Provincia
               
               """
                
CantHabitantes_CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia=dd.query(consulta).df()
#=============================================================================
# Pongo como columnas la cantidad de habitantes de cada rango etario por departamento
#=============================================================================

consulta = """
                SELECT Departamento_id, Departamento, Provincia,
                SUM(CASE WHEN RangoEdad = 'rango_0_5' THEN  Cantidad_Habitantes ELSE 0 END) AS 'Poblacion Jardines'
               ,SUM(CASE WHEN RangoEdad = 'rango_6_12' THEN Cantidad_Habitantes ELSE 0 END) AS 'Poblacion Primarias'
               ,SUM(CASE WHEN RangoEdad = 'rango_13_17' THEN Cantidad_Habitantes ELSE 0 END) AS 'Poblacion Secundarias'
               ,SUM(CASE WHEN RangoEdad = 'mayores_18' THEN Cantidad_Habitantes ELSE 0 END) AS 'Poblacion Superiores No Universitarios'
                FROM CantHabitantes_Por_Departamento_RangoEdades AS C
                GROUP BY Departamento_id, Departamento, Provincia
               
               """
                
CantHabitantes_Por_Departamento_RangoEdades=dd.query(consulta).df()
#=============================================================================
# Hago un Right join entre dos ultimas tablas. Selecciono los datos de la consulta 1.
#=============================================================================

consulta = """
                SELECT CDR.Departamento, CDR.Provincia, jardines, primarias, secundarias, "Superiores No Universitarios", 
                "Poblacion Jardines","Poblacion Primarias","Poblacion Secundarias","Poblacion Superiores No Universitarios"
                FROM CantHabitantes_CantEE_por_NivelEducativoyRangoEdad_por_DepartamentoYProvincia AS CCND
                RIGHT JOIN
                CantHabitantes_Por_Departamento_RangoEdades AS CDR
                ON CDR.Departamento_id=CCND.Departamento_id AND CDR.Departamento=CCND.Departamento AND CDR.Provincia=CCND.Provincia
                ORDER BY CDR.Provincia, "Primarias" DESC                
               """
                
Consulta1=dd.query(consulta).df()

