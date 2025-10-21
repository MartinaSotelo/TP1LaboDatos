#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  9 09:48:09 2025

@author: Estudiante
"""

import pandas as pd
import duckdb as dd
import re
import numpy as np
#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = ""

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)

#%%=========================================================================== 
#         LIMPIEZA DATASETS DE EST. EDUCATIVOS Y EST. PRODUCTIVOS
#=============================================================================
# Hago que coincidan los nombres de provincias en dep_ac_sex y padron_ee
#=============================================================================

consulta = ''' 
             SELECT anio, in_departamentos, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(departamento),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS departamento, provincia_id, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(provincia),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U'), 'CABA', 'CIUDAD DE BUENOS AIRES') AS provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
             FROM dep_ac_sex
             WHERE anio = '2022'
           '''
dep_ac_sex_limpio = dd.query(consulta).df()
#%%
   

# consulta = '''
#               SELECT anio, in_departamentos, departamento, provincia_id, provincia_normalizado, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
#               REPLACE (provincia_normalizado, 'CABA', 'CIUDAD DE BUENOS AIRES' ) as provincia_normalizado_cambia_CABA
#               FROM dep_ac_sex
#            '''       
           
# dep_ac_sex = dd.query(consulta).df()


consulta = ''' 
              SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Jurisdicción),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia, cueanexo AS Cue, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Departamento),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS departamento, "Nivel inicial - Jardín maternal" AS Jardin_maternal, "Nivel inicial - Jardín de infantes" AS Jardin_infantes, Primario, Secundario, SNU, "Secundario - INET" AS Secundario_INET, "SNU - INET" AS SNU_INET,
              FROM padron_ee
              WHERE Común = '1'
           '''        
        
padron_ee = dd.query(consulta).df()
padron_ee.replace(' ', np.nan, inplace=True)
#%%

consulta = '''
              SELECT provincia, Cue,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE
              (departamento,'CORONEL DE MARINA L ROSALES','CORONEL DE MARINA LEONARDO ROSALES' 
              ),'1§ DE MAYO','1° DE MAYO'                                                             
              ),'MAYOR LUIS J FONTANA','MAYOR LUIS J. FONTANA'                                                      
              ),'O HIGGINGS','OHIGGINGS'                                                       
              ),'DOCTOR MANUEL BELGRANO','DR. MANUEL BELGRANO'
              ),'GENERAL OCAMPO','GENERAL ORTIZ DE OCAMPO'                                                     
              ), 'CORONEL FELIPE VARELA','GENERAL FELIPE VARELA'
              ), 'GENERAL ANGEL V PENALOZA','ANGEL VICENTE PENALOZA'
              ),'GENERAL JUAN F QUIROGA','GENERAL JUAN FACUNDO QUIROGA'
              ),'LIBERTADOR GRL SAN MARTIN','LIBERTADOR GENERAL SAN MARTIN'
              ),'GENERAL JUAN MARTIN DE PUEYRREDON', 'JUAN MARTIN DE PUEYRREDON' 
              ),'JUAN B ALBERDI','JUAN BAUTISTA ALBERDI'
              ), 'JUAN F IBARRA','JUAN FELIPE IBARRA'
              ) AS departamento,
              Jardin_maternal, Jardin_infantes, Primario, Secundario, SNU, Secundario_INET, SNU_INET,
              FROM padron_ee
           '''       

padron_ee = dd.query(consulta).df()

consulta = """
               SELECT provincia, departamento, Cue, 
                   CASE 
                       WHEN Jardin_infantes = '1' OR Jardin_maternal = '1' THEN '1'
                       ELSE NULL
                       END AS Jardin,
                   Primario,
                   CASE
                       WHEN Secundario = '1' OR Secundario_INET = '1' THEN '1'
                       ELSE NULL
                       END AS Secundario,
                   CASE
                       WHEN SNU = '1' OR SNU_INET = '1' THEN '1'
                       ELSE NULL
                       END AS SNU
               FROM padron_ee
        """

padron_ee_Limpio = dd.query(consulta).df()

#%%===========================================================================
# Hago que coincidan los nombres de departamento en dep_ac_sex y padron_ee
#=============================================================================
# consulta = ''' 
#              SELECT anio, in_departamentos, departamento, provincia_id, provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
#                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(departamento),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS departamento_sin_tildes
#              FROM dep_ac_sex 
#          '''
#dep_ac_sex = dd.query(consulta).df()

# consulta = ''' 
#               SELECT provincia, cueanexo, Departamento, "Nivel inicial - Jardín maternal" , "Nivel inicial - Jardín de infantes", Primario, Secundario, SNU, "Secundario - INET", "SNU - INET" ,
#               REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Departamento),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS departamento_normalizado
#               FROM padron_ee
             
#            '''
# padron_ee = dd.query(consulta).df()

#%%===========================================================================
# Selecciono las columnas que me interesan y renombro
#=============================================================================
# PADRON ESTABLECIMIENTOS EDUCATIVOS 
#=============================================================================

# consulta = ''' 
#               SELECT provincia AS Provincia, cueanexo AS Cue, departamento, "Nivel inicial - Jardín maternal" AS "Jardin_Maternal", "Nivel inicial - Jardín de infantes" AS "Jardin_Infantes", Primario, Secundario, SNU, "Secundario - INET" AS Secundario_INET, "SNU - INET" AS SNU_INET 
#               FROM padron_ee
             
#            '''
# padron_ee_Limpio = dd.query(consulta).df()

# # Elimino todos las filas de colegios que no sean modalidad común 
# padron_ee_Limpio.replace(' ', np.nan, inplace=True) #reemplazo espacios en blanco por nulls
# padron_ee_Limpio.dropna(thresh=4,inplace=True)

#=============================================================================
# DATOS DEPARTAMENTO POR ACTIVIDAD Y SEXO
#=============================================================================

# consulta = ''' 
#              SELECT in_departamentos, departamento_normalizado, provincia_id, provincia_normalizado_cambia_CABA AS provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
#              FROM dep_ac_sex
#              WHERE anio = '2022';
#            '''
# dep_ac_sex_limpio = dd.query(consulta).df()

#%%===========================================================================
#                    LIMPIEZA DATASET PADRON POBLACION 
#=============================================================================

padron_poblacion.dropna(how='all',inplace=True) #Elimino las tuplas vacias. Reemplazo en el mismo dataframe

'''lo que hago aca es recorrer el data frame, si coincide algun valor con mi match, encontre un código de area,
lo agrego a mi lista para luego insertarla como nueva columna al datraframe, y si no matchea repito el mismo codigo hasta encontrar otro'''

indices_hay_numeroArea=[] #Lista para guardarme los indices
id_areas=[] #esta lista despues la inserto como nueva columna
id_area=0 

for i, row in padron_poblacion.iterrows(): #recorro dataframe
    val = str(row[0])
    match = re.match(r'^AREA\s+#\s*(\d+)', val)
   
    if match:
        numero_area = int(match.group(1))# Devuelve el número ***** y lo pasa a int
        id_areas.append(numero_area)
        indices_hay_numeroArea.append(i) #apendeo el numero nuevo a mi lista
        id_area=numero_area #me guardo el numero para poder apendearlo despues
    else:
        id_areas.append(id_area) #voy apendeando todos los numeros
        

padron_poblacion['id_areas']=id_areas #aca armo mi columna nueva ya con todos los codigos que fui guardando a la lista
padron_poblacion.drop(indices_hay_numeroArea, inplace=True) #Elimino todas las filas donde este el codigo
padron_poblacion=padron_poblacion.drop(padron_poblacion[padron_poblacion['Edad'] == 'Edad'].index) #Elimino filas que repitan edad, casos
padron_poblacion=padron_poblacion.drop(padron_poblacion[padron_poblacion['Edad'] == 'Total'].index) #Elimino filas que digan el total


'''Aca uso la misma tecnica para borrar toda la tabla de Resumen'''


indices_resumen=[] #guardo todos los indices de resumen
indice_comienza_resumen=0 #guardo donde arranca resumen
for i, row in padron_poblacion.iterrows(): #recorro dataframe
    val = str(row[0])
    match2 = re.match(r'RESUMEN', val)
    if match2:
        indice_comienza_resumen=i


for i in range (indice_comienza_resumen,56698):
    indices_resumen.append(i)
    
indices_resumen.remove(56585) #saco los indices que ya elimine
indices_resumen.remove(56586)

padron_poblacion.drop(indices_resumen, inplace=True)
#%%===========================================================================
#Cambio el id de 'ushuaia', 'chascomus' y 'rio grande' para que coincida 
#con el resto de datasets
#=============================================================================
consulta = ''' 
                SELECT Edad, Casos,
                REPLACE(REPLACE(REPLACE(CAST(id_areas AS VARCHAR), '94015', '94014'), '6218' , '6217'), '94008', '94007') AS 'id_areas'
                FROM padron_poblacion;
            '''
padron_poblacion_CambioIds= dd.query(consulta).df()

#%%=============================================================================
#Agrupamos padron_poblacion según el rango de edad.
#=============================================================================
#agrupo los rangos de edad
consulta = ''' 
                SELECT id_areas,
                    CASE
                        WHEN Edad BETWEEN 0 AND 5 THEN '0-5'
                        WHEN Edad BETWEEN 6 AND 12 THEN '6-12'
                        WHEN Edad BETWEEN 13 AND 17 THEN '13-17'
                        ELSE 'Mayores de 18'
                    END AS rango_nombre,
                    SUM(Casos) AS cantidad
                FROM padron_poblacion_CambioIds
                GROUP BY rango_nombre, id_areas
                ORDER BY id_areas,
                    CASE 
                        WHEN rango_nombre = '0-5' THEN 1
                        WHEN rango_nombre = '6-12' THEN 2
                        WHEN rango_nombre = '13-17' THEN 3
                        WHEN rango_nombre = 'Mayores de 18' THEN 4
                    END;
            '''
padron_poblacion_rangos= dd.query(consulta).df()

#armo columnas para cada rango edad
consulta = """
           SELECT 
                 id_areas AS Departamento_id,
                 SUM(CASE WHEN rango_nombre = '0-5' THEN cantidad END) AS rango_0_5,
                 SUM(CASE WHEN rango_nombre = '6-12' THEN cantidad END) AS rango_6_12,
                 SUM(CASE WHEN rango_nombre = '13-17' THEN cantidad END) AS rango_13_17,
                 SUM(CASE WHEN rango_nombre = 'Mayores de 18' THEN cantidad END) AS mayores_18
                 FROM padron_poblacion_rangos
                 GROUP BY id_areas
                 ORDER BY id_areas ASC
           """
padron_poblacion_acomodado = dd.query(consulta).df()




#%%
#Hay que cambiar los nombres de los deptos por los ids de los deptos
# consulta = ''' 
#               SELECT provincia_normalizado AS Provincia, cueanexo AS Cue, departamento_normalizado AS departamento, "Nivel inicial - Jardín maternal" AS "Jardin_Maternal", "Nivel inicial - Jardín de infantes" AS "Jardin_Infantes", Primario, Secundario, SNU, "Secundario - INET" AS Secundario_INET, "SNU - INET" AS SNU_INET 
#               FROM padron_ee
             
#            '''
# padron_ee_Limpio = dd.query(consulta).df()





#%%
#=============================================================================
#Exporto todas las tablas a csv
#=============================================================================
padron_poblacion_acomodado.to_csv("PadronPoblacionLimpio.csv", index=False)
dep_ac_sex_limpio.to_csv("DepartamentoActivdadySexoLimpio.csv", index=False)
padron_ee_Limpio.to_csv("PadronEstablecimientosEducativosLimpio.csv",index=False) #Está la columna id provincia y nombre provincia

