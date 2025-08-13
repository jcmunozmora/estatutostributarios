"""
Created on Sun Jul 13 19:21:52 2025
@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import re

# Ruta base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

ruta_archivo=BASE_DIR/"Inputs"/"Process data"/"2023-CONSOLIDADO EL CARMEN DE VIBORAL.xlsx"           
nombres_hojas = pd.ExcelFile(ruta_archivo).sheet_names       


Columna1=[
    "MUNICIPIO",
    "NUMERO_PREDIAL_NACIONAL",
    "FICHA",
    "MATRICULA",
    "TIPO_DOCUMENTO",
    "NUMERO_DOCUMENTO",
    "DESTINO_ECONOMICO",
    "ZONA",
    "AREA_TERRENO",
    "AREA_CONSTRUIDA",
    "AVALUO_CONSTRUCCION",
    "AVALUO_TERRENO",
    "AVALUO",
    "VIGENCIA",
    "DERECHO",
    "GRAVABLE",
    "AUTOESTIMACION"
]

Cambios={'MPIO':"MUNICIPIO",
         'NPN':"NUMERO_PREDIAL_NACIONAL",
         'FICHA':"FICHA",
         'MATRICULA':"MATRICULA", 
         'TIPO DOCUMENTO':"TIPO_DOCUMENTO",
         'NUMERO DOCUMENTO':"NUMERO_DOCUMENTO", 
         'DESTINACION':"DESTINO_ECONOMICO", 
         'ZONA':"ZONA", 
         'AREA TERRENO':"AREA_TERRENO",
         'AREA CONSTRUCCION':"AREA_CONSTRUIDA", 
         'AVALUO TERRENO':"AVALUO_TERRENO", 
         'AVALUO CONSTRUCCION':"AVALUO_CONSTRUCCION",
         'AVALUO TOTAL':"AVALUO", 
         'DERECHO':"DERECHO", 
         'GRAVABLE':"GRAVABLE", 
         'AUTOESTIMACION':"AUTOESTIMACION",
         'DOCUMENTO':"NUMERO_DOCUMENTO", 
         'AREA TERRENO (PRIVADO + COMUN)':"AREA_TERRENO", 
         'AREA CONSTRUIDA':"AREA_CONSTRUIDA",
         'DESTINACIÓN':"DESTINO_ECONOMICO"}


homologacion = {
    "HABITACIONAL": "Habitacional",
    "AGROPECUARIO": "Agropecuario",
    "AGRICOLA": "Agricola",
    "LOTE RURAL": "Uso publico",
    "LOTE URBANIZADO NO CONSTRUIDO": "Lote urbanizado no construido",
    "PARCELA HABITACIONAL": "Habitacional",
    "COMERCIAL": "Comercial",
    "BIEN DE DOMINIO PUBLICO": "Uso publico",
    "PECUARIO": "Pecuario",
    "VIAS": "Infraestructura transporte",
    "UNIDAD PREDIAL NO CONSTRUIDA": "Por Definir Catastro",
    "INDUSTRIAL": "Industrial",
    "LOTE URBANIZABLE NO URBANIZADO": "Lote urbanizable no urbanizado",
    "RECREACIONAL": "Recreacional",
    "CULTURAL": "Cultural",
    "EDUCATIVO": "Educativo",
    "LOTE NO URBANIZABLE": "Lote no urbanizable",
    "RESERVA FORESTAL": "Forestal",
    "SALUBRIDAD": "Salubridad",
    "RELIGIOSO": "Religioso",
    "INSTITUCIONAL O DEL ESTA": "Institucional",
    "FORESTAL": "Forestal",
    "PARCELA RECREACIONAL": "Recreacional",
    "SERVICIOS ESPECIALES": "Servicios Especiales",
    "PARQUES NACIONALES": "Uso publico",
    "AGROINDUSTRIAL": "Agroindustrial"
}



DATA=pd.DataFrame({})
for anio in nombres_hojas: 
    Data=pd.read_excel(ruta_archivo,engine="openpyxl", dtype=str,sheet_name=anio)
    Data.rename(columns=Cambios,inplace=True)
    Data["VIGENCIA"]=int(anio)
    Data=Data[Columna1]
    DATA=pd.concat([Data,DATA])
    


DATA.GRAVABLE.replace({"SI":"Si","NO":"No"},inplace=True)
DATA.AUTOESTIMACION.replace({"SI":"Si","NO":"No"},inplace=True)
DATA.ZONA.replace({"URBANO":"Urbana","RURAL":"Rural"},inplace=True)
DATA.DESTINO_ECONOMICO.replace(homologacion,inplace=True)


numeros=['FICHA',
'MATRICULA',
#'NUMERO_DOCUMENTO',
'AREA_TERRENO', 
'AREA_CONSTRUIDA', 
'AVALUO_CONSTRUCCION',
'AVALUO_TERRENO',
'AVALUO', 
'VIGENCIA']


for number in  numeros:
    DATA[number]=pd.to_numeric(DATA[number])
DATA=DATA.reset_index()

Codigo="05"+str(DATA.MUNICIPIO[0])    
for anio in nombres_hojas:  
   ruta_archivo=BASE_DIR/"Outputs"/anio           
   os.makedirs(ruta_archivo,exist_ok=True)
   os.chdir(ruta_archivo)
   DATA[DATA.VIGENCIA==int(anio)].drop(columns="index").to_parquet(f"{Codigo}.parquet", index=False)
