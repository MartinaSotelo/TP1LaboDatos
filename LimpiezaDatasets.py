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

carpeta = "/home/martina/Escritorio/tpLabo1/"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
actividades = pd.read_csv(carpeta + "actividades_establecimientos.csv")
dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
padron_poblacion = pd.read_excel(carpeta + "padron_poblacion.xlsX", usecols=[1,2], names=["Edad", "Casos"], skiprows=12)

#%%===========================================================================
# Limpiamos dataset de Padron_poblacion
#=============================================================================

padron_poblacion.dropna(how='all',inplace=True) #Elimino las tuplas vacias. Reemplazo en el mismo dataframe

'''lo que hago aca es recorrer el data frame, si coincide algun valor con mi match, encontre un codigo de area,
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

#%%

'''
despues aca habria que limpiar los nombres de los departamentos, 
fijarnos que coincidan los id (algunos le ponen 0 adelante, otros no) y que coincidan nombres de provincias
'''