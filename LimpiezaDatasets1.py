#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  9 09:48:09 2025

@author: Estudiante
"""

import pandas as pd
import duckdb as dd
import re
#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "C:/Users/perei/Downloads/Datos_para_el_TP/"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)

#%%===========================================================================
# Limpiamos dataset de Padron_poblacion
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

#%% Agrupamos padron_poblacion según el rango de edad.
consulta = ''' 
                SELECT id_areas,
                    CASE
                        WHEN Edad BETWEEN 0 AND 5 THEN '0-5'
                        WHEN Edad BETWEEN 6 AND 12 THEN '6-12'
                        WHEN Edad BETWEEN 13 AND 18 THEN '13-18'
                        ELSE 'Mayores de 18'
                    END AS rango_nombre,
                    SUM(Casos) AS cantidad
                FROM padron_poblacion
                GROUP BY rango_nombre, id_areas
                ORDER BY id_areas,
                    CASE 
                        WHEN rango_nombre = '0-5' THEN 1
                        WHEN rango_nombre = '6-12' THEN 2
                        WHEN rango_nombre = '13-18' THEN 3
                        WHEN rango_nombre = 'Mayores de 18' THEN 4
                    END;
            '''
padron_poblacion = dd.query(consulta).df()

#%%

consulta = """
           SELECT 
                 id_areas,
                 SUM(CASE WHEN rango_nombre = '0-5' THEN cantidad END) AS rango_0_5,
                 SUM(CASE WHEN rango_nombre = '6-12' THEN cantidad END) AS rango_6_12,
                 SUM(CASE WHEN rango_nombre = '13-18' THEN cantidad END) AS rango_13_18,
                 SUM(CASE WHEN rango_nombre = 'Mayores de 18' THEN cantidad END) AS mayores_18
                 FROM padron_poblacion
                 GROUP BY id_areas
                 ORDER BY id_areas ASC

           """
padron_poblacion_new = dd.query(consulta).df()
padron_poblacion_new.to_csv("Padron_poblacion_new_acomodado.csv", index=False)
#%%

'''
despues aca habria que limpiar los nombres de los departamentos, 
fijarnos que coincidan los id (algunos le ponen 0 adelante, otros no) y que coincidan nombres de provincias
'''

consulta = """
               SELECT clae6, clae6_desc
               FROM actividades
        """

actividades = dd.query(consulta).df()
#%%
#Pasar minusculas a mayusculas
#Ciudad de Buenos Aires a CABA
#Sacar las tildes


# consulta = """
#                 SELECT anio, in_departamentos, UPPER(departamento) AS departamento, provincia_id, clae6, genero, Empleo, Establecimientos, empresas_exportadoras
#                 FROM dep_ac_sex;
#             """

# dep_ac_sex = dd.query(consulta).df()


consulta = """
                SELECT in_departamentos, UPPER(departamento) AS departamento
                FROM dep_ac_sex;
            """

departamento = dd.query(consulta).df()

consulta = """
                SELECT DISTINCT provincia_id, provincia
                FROM dep_ac_sex;
            """

provincia = dd.query(consulta).df()

consulta = """
                SELECT Cueanexo
                FROM padron_ee;
            """

establecimiento_educativo = dd.query(consulta).df()


nivel_educativo = pd.DataFrame({'nivel_educativo': ['Jardines', 'Primarios', 'Secundarios']})

consulta = """
                SELECT Cueanexo
                FROM padron_ee;
            """

establecimiento_educativo = dd.query(consulta).df()
