#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 18 13:30:00 2025

@author: martina
"""


import pandas as pd
import duckdb as dd
import re
import numpy as np

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================
carpeta = "/home/martina/Escritorio/tpLabo1/"

padron_ee = pd.read_excel(carpeta + "2022_padron_oficial_establecimientos_educativos.xlsx", skiprows=6)
EE_Limpio = pd.read_csv(carpeta+"PadronEstablecimientosEducativosLimpio.csv")
#=============================================================================
padron_ee.replace(' ', np.nan, inplace=True)

consulta= """ 
              SELECT COUNT(Común)
              FROM padron_ee
              WHERE Común IS NOT NULL;
              """
              
filas_de_comun=dd.query(consulta).df()
print(filas_de_comun)
