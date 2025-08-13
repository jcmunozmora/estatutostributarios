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
    Informacion_por_anio[anio]=os.listdir(ruta_input/anio)    
todos_los_valores = set(v for lista in Informacion_por_anio.values() for v in lista)
# Crear la matriz de presencia
df = pd.DataFrame(index=sorted(todos_los_valores), columns=Informacion_por_anio.keys())

# Llenar con 'X' si el valor está en la lista correspondiente
for col, lista in Informacion_por_anio.items():
    df.loc[df.index.isin(lista), col] = 'X'

# Opcional: reemplazar los NaN por vacío
df.fillna('', inplace=True)


df.to_excel(BASE_DIR /"Archivos_faltantes.xlsx",index=True)



Consolidado=pd.DataFrame({})

for anio in  list(range(2025, 2018, -1)):
    Archivos=os.listdir(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/R1")
    archivos_sin_extension = [archivo.replace('.xlsx', '') for archivo in Archivos]
    Temporal=pd.DataFrame({"Codigo":archivos_sin_extension})
    Temporal[f"{anio}"]=1
    if anio==2025:          
       Consolidado=pd.concat([Consolidado,Temporal])
    else:
        Consolidado=Consolidado.merge(Temporal,how="left",on="Codigo")
Consolidado.to_excel("C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Reporte_municipios_R1.xlsx")



Consolidado=pd.DataFrame({})

for anio in  list(range(2021, 2014, -1)):
    Archivos=os.listdir(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/R1_")
    archivos_sin_extension = [archivo.replace('.xlsx', '') for archivo in Archivos]
    Temporal=pd.DataFrame({"Codigo":archivos_sin_extension})
    Temporal[f"{anio}"]=1
    if anio==2021:          
       Consolidado=pd.concat([Consolidado,Temporal])
    else:
        Consolidado=Consolidado.merge(Temporal,how="left",on="Codigo")
Consolidado.to_excel("C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Reporte_municipios_R1_.xlsx")



Consolidado=pd.DataFrame({})

for anio in  list(range(2022, 2014, -1)):
    Archivos=os.listdir(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/TXT")
    archivos_sin_extension = [archivo.replace('.xlsx', '') for archivo in Archivos]
    Temporal=pd.DataFrame({"Codigo":archivos_sin_extension})
    Temporal[f"{anio}"]=1
    if anio==2022:          
       Consolidado=pd.concat([Consolidado,Temporal])
    else:
        Consolidado=Consolidado.merge(Temporal,how="left",on="Codigo")
Consolidado.to_excel("C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Reporte_municipios_TXT.xlsx")




dfs={}

dfs["Pedido_inicial"]=pd.DataFrame(
{"AREA_CONSTRUIDA":[],
"AREA_TERRENO":[],
"AVALUO":[],
"DESTINO_ECONOMICO":[],
"MUNICIPIO":[],
"NUMERO_DE_ORDEN":[],
"NUMERO_DOCUMENTO":[],
"NUMERO_PREDIAL_ANTERIOR":[],
"NUMERO_PREDIAL_NACIONAL":[],
"TIPO_DOCUMENTO":[],
"TOTAL_REGISTROS":[],
"VIGENCIA":[],
"ZONA":[],
"FICHA":[],
"DERECHO":[]
})


dfs["Ideal"]=pd.DataFrame(
{"AREA_CONSTRUIDA":[],
"AREA_TERRENO":[],
"AVALUO":[],
"DESTINO_ECONOMICO":[],
"MUNICIPIO":[],
"NUMERO_DE_ORDEN":[],
"NUMERO_DOCUMENTO":[],
"NUMERO_PREDIAL_ANTERIOR":[],
"NUMERO_PREDIAL_NACIONAL":[],
"TIPO_DOCUMENTO":[],
"TOTAL_REGISTROS":[],
"VIGENCIA":[],
"ZONA":[],
"FICHA":[],
"DERECHO":[],
"NUEVO":[],
"AUTOESTIMACION":[],
"GRAVABLE":[],
"ESTRATO":[],
"AUTOESTIMACION":[],
"TARIFA":[],
"LIQUIDACION":[]

})







for anio in  list(range(2025, 2018, -1)):
    Archivos=os.listdir(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/R1")
    archivo=Archivos[0]
    dfs[anio]=pd.read_excel(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/R1/{archivo}",nrows=2)

todas_columnas = set()
for df in dfs.values():
    todas_columnas.update(df.columns)

# Crear DataFrame comparativo
comparativo = pd.DataFrame(index=sorted(todas_columnas), columns=dfs.keys())

# Llenar con "X" si la columna está en el DataFrame
for nombre, df in dfs.items():
    comparativo[nombre] = pd.Series(comparativo.index.isin(df.columns), index=comparativo.index).map(lambda x: 'X' if x else '')

comparativo.to_excel(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Contenido_archivos_R1.xlsx")


dfs={}

dfs["Pedido_inicial"]=pd.DataFrame(
{"AREA_CONSTRUIDA":[],
"AREA_TERRENO":[],
"AVALUO":[],
"DESTINO_ECONOMICO":[],
"MUNICIPIO":[],
"NUMERO_DE_ORDEN":[],
"NUMERO_DOCUMENTO":[],
"NUMERO_PREDIAL_ANTERIOR":[],
"NUMERO_PREDIAL_NACIONAL":[],
"TIPO_DOCUMENTO":[],
"TOTAL_REGISTROS":[],
"VIGENCIA":[],
"ZONA":[],
"FICHA":[],
"DERECHO":[]
})


dfs["Ideal"]=pd.DataFrame(
{"AREA_CONSTRUIDA":[],
"AREA_TERRENO":[],
"AVALUO":[],
"DESTINO_ECONOMICO":[],
"MUNICIPIO":[],
"NUMERO_DE_ORDEN":[],
"NUMERO_DOCUMENTO":[],
"NUMERO_PREDIAL_ANTERIOR":[],
"NUMERO_PREDIAL_NACIONAL":[],
"TIPO_DOCUMENTO":[],
"TOTAL_REGISTROS":[],
"VIGENCIA":[],
"ZONA":[],
"FICHA":[],
"DERECHO":[],
"NUEVO":[],
"AUTOESTIMACION":[],
"GRAVABLE":[],
"ESTRATO":[],
"AUTOESTIMACION":[],
"TARIFA":[],
"LIQUIDACION":[]

})









for anio in  list(range(2021, 2014, -1)):
    Archivos=os.listdir(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/R1_")
    archivo=Archivos[0]
    dfs[anio]=pd.read_excel(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/R1_/{archivo}",nrows=2)
todas_columnas = set()
for df in dfs.values():
    todas_columnas.update(df.columns)
# Crear DataFrame comparativo
comparativo = pd.DataFrame(index=sorted(todas_columnas), columns=dfs.keys())
# Llenar con "X" si la columna está en el DataFrame
for nombre, df in dfs.items():
    comparativo[nombre] = pd.Series(comparativo.index.isin(df.columns), index=comparativo.index).map(lambda x: 'X' if x else '')
comparativo.to_excel(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Contenido_archivos_R1_.xlsx")



dfs={}
dfs["Pedido_inicial"]=pd.DataFrame(
{"AREA_CONSTRUIDA":[],
"AREA_TERRENO":[],
"AVALUO":[],
"DESTINO_ECONOMICO":[],
"MUNICIPIO":[],
"NUMERO_DE_ORDEN":[],
"NUMERO_DOCUMENTO":[],
"NUMERO_PREDIAL_ANTERIOR":[],
"NUMERO_PREDIAL_NACIONAL":[],
"TIPO_DOCUMENTO":[],
"TOTAL_REGISTROS":[],
"VIGENCIA":[],
"ZONA":[],
"FICHA":[],
"DERECHO":[]
})


dfs["Ideal"]=pd.DataFrame(
{"AREA_CONSTRUIDA":[],
"AREA_TERRENO":[],
"AVALUO":[],
"DESTINO_ECONOMICO":[],
"MUNICIPIO":[],
"NUMERO_DE_ORDEN":[],
"NUMERO_DOCUMENTO":[],
"NUMERO_PREDIAL_ANTERIOR":[],
"NUMERO_PREDIAL_NACIONAL":[],
"TIPO_DOCUMENTO":[],
"TOTAL_REGISTROS":[],
"VIGENCIA":[],
"ZONA":[],
"FICHA":[],
"DERECHO":[],
"NUEVO":[],
"AUTOESTIMACION":[],
"GRAVABLE":[],
"ESTRATO":[],
"AUTOESTIMACION":[],
"TARIFA":[],
"LIQUIDACION":[]

})




for anio in  list(range(2022, 2014, -1)):
    Archivos=os.listdir(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/TXT")
    archivo=Archivos[0]
    dfs[anio]=pd.read_excel(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Outputs/{anio}/TXT/{archivo}",nrows=2)
todas_columnas = set()
for df in dfs.values():
    todas_columnas.update(df.columns)
# Crear DataFrame comparativo
comparativo = pd.DataFrame(index=sorted(todas_columnas), columns=dfs.keys())
# Llenar con "X" si la columna está en el DataFrame
for nombre, df in dfs.items():
    comparativo[nombre] = pd.Series(comparativo.index.isin(df.columns), index=comparativo.index).map(lambda x: 'X' if x else '')
comparativo.to_excel(f"C:/Users/danie/OneDrive/Documentos/GitHub/estatutostributarios/Clean/Contenido_archivos_TXT.xlsx")
