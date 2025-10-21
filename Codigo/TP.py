import pandas as pd
import duckdb as dd
import re
import numpy as np

#%%
carpeta = ""
import pandas as pd
import duckdb as dd
import re
import numpy as np

#dep_ac_sex = pd.read_csv(carpeta + "Datos_por_departamento_actividad_y_sexo.csv")
#padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
EE_Limpio = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
EP_Limpio = pd.read_csv(carpeta+"DepartamentoActivdadySexoLimpio.csv")
actividades = pd.read_csv(carpeta+"actividades_establecimientos.csv")
padron_poblacion = pd.read_csv(carpeta+"PadronPoblacionLimpio.csv")

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

#%%

# consulta = """
#                SELECT EE_Limpio.Provincia, Cue, UPPER(departamento) AS departamento, Jardin_Infantes, Jardin_Maternal Primario, Secundario, Secundario_INET, SNU, SNU_INET
#                FROM EE_Limpio
#         """

# EE_Limpio = dd.query(consulta).df()

#%%

# consulta = """
#                SELECT DISTINCT provincia, provincia_id
#                FROM EP_Limpio
#         """

# Provincia = dd.query(consulta).df()

#%%

# consulta = """
#                SELECT DISTINCT UPPER(departamento) AS departamento, in_departamentos
#                FROM EP_Limpio
#         """

# Departamento = dd.query(consulta).df()

#%%

# consulta = """
#                SELECT DISTINCT in_departamentos,UPPER(departamento) AS departamento, provincia_id, provincia
#                FROM EP_Limpio
#         """

# Pertenece_a = dd.query(consulta).df()

#%%

# consulta = """
#                 SELECT Cue
#                 FROM EE_Limpio;
#             """

# Establecimiento_educativo = dd.query(consulta).df()

#%%


# Crear tabla
# dd.query("""
#     DROP TABLE IF EXISTS Nivel_educativo;
#     CREATE TABLE Nivel_educativo (
#         Nivel VARCHAR(100) PRIMARY KEY
#     );
# """)

# Insertar datos
# dd.query("""
#     INSERT INTO Nivel_educativo (Nivel)
#     VALUES ('Jardin'), ('Primario'), ('Secundario');
# """)

# # Ver el resultado
# Nivel_educativo = dd.query("SELECT * FROM Nivel_educativo").df()


#%%

'''Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de Establecimientos Educativos (EE) de cada nivel educativo,
considerando solamente la modalidad común, y la cantidad de habitantes con
edad correspondiente al nivel educativos listado.El orden del reporte debe 
ser alfabético por provincia y dentro de las provincias descendente por
cantidad de escuelas primarias.'''


consulta = """
                SELECT Pertenece_a.in_departamentos, EE_Limpio.Provincia, EE_Limpio.departamento, COUNT(Jardin_Infantes) AS Jardines, COUNT(Primario) AS Primarias, COUNT(Secundario) AS Secundarios, padron_poblacion_new.rango_0_5 AS Poblacion_Jardin, rango_6_12 AS Poblacion_Primaria, rango_13_17 AS Poblacion_Secundaria 
                FROM EE_Limpio
                JOIN Pertenece_a ON EE_Limpio.departamento = Pertenece_a.departamento AND EE_Limpio.Provincia = Pertenece_a.provincia
                JOIN padron_poblacion_new ON Pertenece_a.in_departamentos = padron_poblacion_new.id_areas
                GROUP BY Pertenece_a.in_departamentos, EE_Limpio.Provincia, EE_Limpio.departamento, rango_0_5, rango_6_12, rango_13_17
                ORDER BY EE_Limpio.Provincia ASC, Primarias DESC        
            """
            

Consulta_1 = dd.query(consulta).df()
#%%

