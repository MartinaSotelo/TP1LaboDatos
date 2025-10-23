"""
Grupo Import_milanesas
Integrantes: 
Dulio Joaquin, 
Risuleo Franco, 
Perez Sotelo Martina

En este archivo realizamos las visualizaciones pedidas:

    Las tablas del modelo estan importadas desde el archivo .py
"""
#%%===========================================================================
# Importamos librerias vamos a usar
#=============================================================================
import duckdb as dd
import os

#%%===========================================================================
# Importamos todas las tablas del modelo
#=============================================================================

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
                
CONSULTA1=dd.query(consulta).df()

print('Consulta 1:')
print(CONSULTA1)

# %%

#%%===========================================================================
#                            CONSULTA 2
#=============================================================================
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
CONSULTA2=dd.query(consulta).df()

print('Consulta 2:')
print(CONSULTA2)

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

#Cuento por departamento cantidad de empresas exportadoras que emplean mujeres

consulta= ''' 
            SELECT Departamento_id, SUM(empresas_exportadoras_mujeres) AS EmpresasExportadoras_EmpleanMujeres
            FROM ActividadProductiva_Departamento
            GROUP BY Departamento_id
         '''
        
Departamento_Cant_EmpExp_Mujeres= dd.query(consulta).df()

#Cuento por departamento cantidad de Est Educativos e indico poblacion total
consulta= ''' 
            SELECT Departamento_id, COUNT(Cue) AS "Cantidad de EE"
            FROM Departamento_EstablecimientoEducativo
            GROUP BY Departamento_id
            
         '''
        
Departamento_Cant_EE= dd.query(consulta).df()

#Cuento por deparatmento Cantidad de habitantes
consulta= ''' 
            SELECT Departamento_id, SUM(Cantidad_Habitantes) AS Poblacion
            FROM Departamento_RangoEdades
            GROUP BY Departamento_id
            
         '''
        
Departamento_Poblacion= dd.query(consulta).df()


# hago un join de Departamento_poblacion y Departamento_Cant_EE
consulta= ''' 
            SELECT DP.Departamento_id, Poblacion , "Cantidad de EE"
            FROM Departamento_Poblacion AS DP
            LEFT JOIN 
            Departamento_Cant_EE AS EE
            ON DP.Departamento_id=EE.Departamento_id
            
         '''
        
Dep_poblacion_cantEE= dd.query(consulta).df()

# hago un join de Dep_poblacion_cantEE y Departamento_Cant_EmpExp_Mujeres
consulta= ''' 
            SELECT PEE.Departamento_id, Poblacion , "Cantidad de EE", EmpresasExportadoras_EmpleanMujeres
            FROM Dep_poblacion_cantEE AS PEE
            LEFT JOIN 
            Departamento_Cant_EmpExp_Mujeres AS XM
            ON XM.Departamento_id=PEE.Departamento_id
            
         '''
        
Dep_poblacion_cantEE_EmpEx_Mujeres= dd.query(consulta).df()


# tabla anterior le agrego nombre provincia y departamento
consulta= ''' 
            SELECT Departamento, Provincia, Poblacion , "Cantidad de EE", EmpresasExportadoras_EmpleanMujeres
            FROM Dep_poblacion_cantEE_EmpEx_Mujeres AS DCX
            INNER JOIN 
            ProvinciaYDepartamento AS PYD
            ON PYD.Departamento_id=DCX.Departamento_id
            ORDER BY "Cantidad de EE" DESC, EmpresasExportadoras_EmpleanMujeres DESC, Provincia, departamento
            
         '''
        
CONSULTA3= dd.query(consulta).df()
print('Consulta 3:')
print(CONSULTA3)

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

CONSULTA4=dd.query(consulta).df()
print('Consulta 4:')
print(CONSULTA4)

#=============================================================================
#Exporto todas las tablas a csv
#=============================================================================
#Creo la carpeta Consultas si no existe
os.makedirs("Consultas", exist_ok=True)

CONSULTA4.to_csv("Consultas/Consulta4.csv", index=False)
CONSULTA3.to_csv("Consultas/Consulta3.csv", index=False)
CONSULTA2.to_csv("Consultas/Consulta2.csv", index=False)
CONSULTA1.to_csv("Consultas/Consulta1.csv", index=False)

#=============================================================================
# Hago las mismas consultas pero viendo el resultado total en cada provincia
#=============================================================================

consulta= ''' 
              SELECT Provincia, SUM(jardines) AS jardines, SUM(primarias) as primarias, SUM(secundarias) as secundarias, SUM("Superiores No Universitarios") as SNU,
              SUM("Poblacion jardines"), SUM("poblacion primarias"), SUM("Poblacion secundarias"), SUM("poblacion superiores no universitarios")
              FROM CONSULTA1 
              GROUP BY Provincia
              ORDER BY (SUM("poblacion superiores no universitarios") + SUM("Poblacion secundarias") +SUM("poblacion primarias")+ SUM("Poblacion jardines")) DESC
          '''
Provincia_Consulta1=dd.query(consulta).df()
Provincia_Consulta1.to_csv("Consultas/Provincia_Consulta1.csv", index=False)


consulta= ''' 
              SELECT provincia, SUM(EmpleadosTotales) AS EmpleadosTotales,
              FROM CONSULTA2 
              GROUP BY provincia
              ORDER BY EmpleadosTotales DESC
          '''
Provincia_Consulta2=dd.query(consulta).df()
Provincia_Consulta2.to_csv("Consultas/Provincia_Consulta2.csv", index=False)

consulta= ''' 
              SELECT Provincia, SUM(poblacion) , SUM("Cantidad de EE"), SUM(EmpresasExportadoras_EmpleanMujeres)
              FROM CONSULTA3
              GROUP BY Provincia, poblacion, "Cantidad de EE" , EmpresasExportadoras_EmpleanMujeres 
              ORDER BY Poblacion DESC, "Cantidad de EE" DESC, EmpresasExportadoras_EmpleanMujeres DESC
          '''
Provincia_Consulta3=dd.query(consulta).df()
Provincia_Consulta3.to_csv("Consultas/Provincia_Consulta3.csv", index=False)


consulta= ''' 
              SELECT Provincia, clae3, SUM(empleados)
              FROM CONSULTA4
              GROUP BY Provincia, clae3 
              ORDER BY Provincia, SUM(empleados) DESC
          '''
Provincia_Consulta4=dd.query(consulta).df()
Provincia_Consulta4.to_csv("Consultas/Provincia_Consulta4.csv", index=False)
