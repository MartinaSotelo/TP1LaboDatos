#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
En este archivo hacemos una limpieza de las tablas originales.

Normalizamos los nombres de las provincias y departamentos
guiandonos del dataset 'departamentos'.

"""
#%%===========================================================================
# Importamos librerias vamos a usar
#=============================================================================
import pandas as pd
import duckdb as dd
import re
import numpy as np
#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "/Import_Milanesas/TablasOriginales"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)
DepartamentoNormalizado = pd.read_csv(carpeta +"departamentos.csv")

#===========================================================================
#                          TABLA DE NORMALIZACION 
#%%=========================================================================== 
# Tomo las columnas nombre dpto, nombre provincia y id's correspondientes
# de DepartamentoNormalizado
#===========================================================================
consulta = ''' 
             SELECT id AS Departamento_id, nombre AS departamento, provincia_id, provincia_nombre AS provincia
             FROM DepartamentoNormalizado
           '''
Normalizacion = dd.query(consulta).df()

#%%===========================================================================
#              LIMPIEZA DATASET DEPARTAMENTOS POR ACTIVIDAD Y SEXO
#=============================================================================

#corrijo los ids  para que sean iguales al id del normalizado 
#(sino voy a perder estos 3 departamentos en mi limpieza)
dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(6217, 6218)
dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(94007, 94008)
dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(94014, 94015)


# Normalizo nombres de departamento y provincia
consulta = ''' 
             SELECT N.departamento_id, N.departamento, N.provincia_id, N.provincia, clae6, genero, Empleo, establecimientos, empresas_exportadoras,
             FROM dep_ac_sex AS X
             INNER JOIN
             Normalizacion AS N
             ON in_departamentos = N.departamento_id 
             WHERE anio = '2022'
           '''
dep_ac_sex_normalizado = dd.query(consulta).df()

# le agrego la descripcion del clae6
consulta = ''' 
             SELECT Departamento_id, departamento, provincia_id, provincia, A.clae6, clae6_desc, genero, Empleo, establecimientos, empresas_exportadoras,
             FROM dep_ac_sex_normalizado AS X
             INNER JOIN
             actividades AS A
             ON X.clae6 = A.clae6
             
           '''
dep_ac_sex_limpio = dd.query(consulta).df()


#%%===========================================================================
#                    LIMPIEZA DATASET PADRON ESTABLECIMIENTOS EDUCATIVOS
#=============================================================================
# padron_ee: pongo NULLS donde hay espacios en blanco.
padron_ee.replace(' ', np.nan, inplace=True)


# le saco tildes y pongo mayusculas a los nombres de provincias y departamentos a
consulta = ''' 
              SELECT Común, cueanexo, Departamento, "Nivel inicial - Jardín maternal" AS JardinMaternal, "Nivel inicial - Jardín de infantes" AS JardinInfantes, Primario, Secundario, SNU, "Secundario - INET" AS SecundarioInet, "SNU - INET" AS SNUInet,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Jurisdicción),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia
              FROM padron_ee
             
           '''
padron_ee_mayus_sinTildes1 = dd.query(consulta).df()

consulta = ''' 
              SELECT común, provincia, cueanexo, Departamento, JardinMaternal , JardinInfantes, Primario, Secundario, SNU, SecundarioInet, SnuInet,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(Departamento),'á', 'a'),'é', 'e'),'í', 'i'),'ó', 'o'),'ú', 'u') AS departamento_normalizado
              FROM padron_ee_mayus_sinTildes1
             
           '''
padron_ee_mayus_sinTildes2 = dd.query(consulta).df()

consulta = ''' 
              SELECT departamento_id,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(departamento),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS departamento,
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(provincia),'Á', 'A'),'É', 'E'),'Í', 'I'),'Ó', 'O'),'Ú', 'U') AS provincia,
              provincia_id
              FROM Normalizacion
             
           '''
Normalizacion_mayus_sinTildes = dd.query(consulta).df()

consulta = ''' 
             SELECT  P.provincia, P.Departamento, N.departamento, N.provincia
             FROM padron_ee_mayus_sinTildes2 AS P
             
             LEFT JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             WHERE N.departamento IS NULL AND P.Departamento  NOT LIKE 'Comuna%'
             GROUP BY N.departamento, N.provincia, P.provincia, P.Departamento
             
             
           '''
diferencia = dd.query(consulta).df()


dep_ac_sex['in_departamentos'] = dep_ac_sex['in_departamentos'].replace(6217, 6218)

#=============================================================================
# CORRIJO LOS NOMBRES QUE DIFIEREN ENtre PADRON_EE y normalizacion
#=============================================================================

padron_ee_mayus_sinTildes2['provincia'] = padron_ee_mayus_sinTildes2['provincia'].replace("TIERRA DEL FUEGO","TIERRA DEL FUEGO, ANTARTIDA E ISLAS DEL ATLANTICO SUR")
padron_ee_mayus_sinTildes2['provincia'] = padron_ee_mayus_sinTildes2['provincia'].replace("CIUDAD DE BUENOS AIRES","CIUDAD AUTONOMA DE BUENOS AIRES")

CORRECCIONES_DEPARTAMENTO = {
    
    "GENERAL GUEMES": "GENERAL GÜEMES", 
    "GENERAL JUAN F QUIROGA": "GENERAL JUAN FACUNDO QUIROGA",
    "MALARGUE": "MALARGÜE",
    "LIBERTADOR GRL SAN MARTIN": "LIBERTADOR GENERAL SAN MARTIN", 
    "TOLHUIN": "TOLHUIN",
    "CORONEL DE MARINA L ROSALES": "CORONEL DE MARINA LEONARDO ROSALES",
    "1§ DE MAYO": "1° DE MAYO",
    "1 DE MAYO": "1° DE MAYO",
    "DOCTOR MANUEL BELGRANO": "DR. MANUEL BELGRANO",
    "DR MANUEL BELGRANO": "DR. MANUEL BELGRANO",
    "CORONEL FELIPE VARELA": "GENERAL FELIPE VARELA",
    "GENERAL ANGEL V PEÑALOZA": "ANGEL VICENTE PEÑALOZA" ,
    "GENERAL JUAN MARTIN DE PUEYRREDON": "JUAN MARTIN DE PUEYRREDON",
    "JUAN MARTIN DE PUEYRREDON": "JUAN MARTÍN DE PUEYRREDÓN", 
    "USHUAIA": "USHUAIA",
    "MAYOR LUIS J FONTANA": "MAYOR LUIS J. FONTANA",
    "ANTÁRTIDA ARGENTINA":"ANTARTIDA ARGENTINA",
    "JUAN F IBARRA": "JUAN FELIPE IBARRA",
    "O HIGGINS": "O'HIGGINS",
    "GENERAL OCAMPO": "GENERAL ORTIZ DE OCAMPO",
    "GUER AIKE": "GÜER AIKE",
    "JUAN B ALBERDI": "JUAN BAUTISTA ALBERDI",
    "Comuna 6" :"COMUNA 6",
	"Comuna 1":"COMUNA 1",
	"Comuna 4":"COMUNA 4",
    "Comuna 5":"COMUNA 5",
    "Comuna 12":"COMUNA 12",
    "Comuna 2": "COMUNA 2",
    "Comuna 10"	:"COMUNA 10",
	"Comuna 11":"COMUNA 11",
	"Comuna 3": "COMUNA 3",
	"Comuna 2":"COMUNA 2",
    "Comuna 8": "COMUNA 8",
	"Comuna 13":"COMUNA 13",
	"Comuna 14":"COMUNA 14",
	"Comuna 9":"COMUNA 9",
	"Comuna 15":"COMUNA 15",
    "Comuna 7":"COMUNA 7"
}


padron_ee_mayus_sinTildes2['Departamento'] = padron_ee_mayus_sinTildes2['Departamento'].replace(CORRECCIONES_DEPARTAMENTO)

#============================================================================
# Recupero el Id y normalizo nombres de la tabla
#============================================================================
#normalizo
consulta = ''' 
             SELECT N.departamento_id As departamento_id, común,  cueanexo, JardinMaternal , JardinInfantes, Primario, Secundario, SNU, SecundarioInet, SnuInet,
             FROM padron_ee_mayus_sinTildes2 AS P
             
             INNER JOIN
             
             Normalizacion_mayus_sinTildes AS N 
             ON N.departamento = P.departamento AND N.provincia = P.provincia
             
           '''
Padron_ee_conIDDepto = dd.query(consulta).df()
#============================================================================
#recupero todas las columnas
#============================================================================
consulta = ''' 
             SELECT N.departamento_id, N.departamento, N.provincia,  común,  cueanexo, JardinMaternal , JardinInfantes, Primario, Secundario, SNU, SecundarioInet, SnuInet,
             FROM Padron_ee_conIDDepto AS P
             
             INNER JOIN
             
             Normalizacion AS N 
             ON N.departamento_id = P.departamento_id
             
           '''
Padron_ee_limpio = dd.query(consulta).df()

#============================================================================
#Junto todos los jardines, secundarios y superiores
#============================================================================
consulta = """
               SELECT provincia, departamento, departamento_id, Cueanexo, 
                   CASE 
                       WHEN JardinInfantes = '1' OR JardinMaternal = '1' THEN '1'
                       ELSE NULL
                       END AS Jardin,
                   Primario,
                   CASE
                       WHEN Secundario = '1' OR SecundarioInet = '1' THEN '1'
                       ELSE NULL
                       END AS Secundario,
                   CASE
                       WHEN SNU = '1' OR SNUInet = '1' THEN '1'
                       ELSE NULL
                       END AS SNU
               FROM Padron_ee_limpio
        """

Padron_ee_limpio = dd.query(consulta).df()

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

#%%=============================================================================
#Agrupamos padron_poblacion según el rango de edad.
#=============================================================================
#agrupo los rango de edad
consulta = ''' 
                SELECT id_areas,
                    CASE
                        WHEN Edad BETWEEN 0 AND 5 THEN '0-5'
                        WHEN Edad BETWEEN 6 AND 12 THEN '6-12'
                        WHEN Edad BETWEEN 13 AND 17 THEN '13-17'
                        ELSE 'Mayores de 18'
                    END AS rango_nombre,
                    SUM(Casos) AS cantidad
                FROM padron_poblacion
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

