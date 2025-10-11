#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 11 00:04:48 2025

@author: martina
"""

import pandas as pd
import duckdb as dd
import re
#%%===========================================================================
# Importamos los dataFrames limpios que vamos a utilizar.
#=============================================================================

from spyder import padron_poblacion, padron_ee, dep_ac_sex, actividades


consulta = """
               SELECT DISTINCT provincia_id, provincia
               FROM dep_ac_sex;
        """

info_provincia = dd.query(consulta).df() #id de provincia -> nombre de provincia
#%%

consulta = """
               SELECT DISTINCT in_departamento, departamento, provincia_id
               FROM dep_ac_sex;
        """

info_depto = dd.query(consulta).df() #id de departamento, nombre de departamento y provincia a la que pertenece
print(info_depto)
