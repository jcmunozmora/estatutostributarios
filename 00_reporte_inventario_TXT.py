# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 22:09:04 2025
@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import zipfile
import numpy as np
import re

BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta relativa al archivo
ruta_input = BASE_DIR /"Outputs"
Anios=os.listdir(ruta_input)
Informacion_por_anio={}
for anio in Anios:
    Informacion_por_anio[anio]=[f for f in os.listdir(ruta_input/anio) if f.lower().endswith('.parquet')]    
todos_los_valores = set(v for lista in Informacion_por_anio.values() for v in lista)
# Crear la matriz de presencia
df = pd.DataFrame(index=sorted(todos_los_valores), columns=Informacion_por_anio.keys())

# Llenar con 'X' si el valor está en la lista correspondiente
for col, lista in Informacion_por_anio.items():
    df.loc[df.index.isin(lista), col] = 'X'

# Opcional: reemplazar los NaN por vacío
df.fillna('', inplace=True)

df=df.reset_index()
df["index"]=df["index"].str.replace(".parquet", "", regex=False)

datos=pd.read_excel("C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Documentación/Priorización.xlsx")
datos["Codigo"] = datos["Codigo"].astype(str).str.zfill(5)
resultado = datos.merge(df, left_on="Codigo", right_on="index", how="left")
resultado.to_excel(BASE_DIR /"Inventario.xlsx",index=False)







