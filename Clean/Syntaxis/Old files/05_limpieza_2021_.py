# -*- coding: utf-8 -*-
"""
Created on Wed Jul 23 20:50:31 2025

@author: danie
"""

"""
Created on Sun Jul 13 19:21:52 2025

@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import zipfile
import numpy as np
import re


BASE_DIR = Path(__file__).resolve().parent.parent

anio=2021
route_explore = BASE_DIR /"Inputs"/"Route data"/"Information_2015_2023"/str(anio)
if anio==2015:
    route_explore=route_explore/"REG_BASICO"

TXTs = [f for f in os.listdir(route_explore) if f.lower().endswith('.txt')]

R1=[f for f in os.listdir(route_explore) if  f.startswith('05_REG_BASICO')]
R2=[f for f in os.listdir(route_explore) if f.startswith('05_REG_COMPLEMENTARIO')]

os.chdir(route_explore)

Consolidado = pd.DataFrame()
i = 0
campos = ["MPIO", "SECTOR", "CORREGIM", "BARRIO", "MANZ_VEREDA", "PREDIO", "EDIFICIO",
  "UNIDAD", "FICHA", "NOMBRE", "APELLIDOS1", "APELLIDOS2", "TIPO_DOC", "DOCUMENTO",
  "DIRECCION", "DESTINO", "AREATERR", "AREACONST", "AVALUO", "MODOAQUISICION",
  "CIRCULO", "MATRICULA", "FECHAREGISTRO", "ESCRITURA", "FECHAESCRITURA",
  "VIGENCIA", "NPN","DERECHO"]


cambio_campos = {
    #"DEPARTAMENTO":"DEPARTAMENTO",
    "MPIO":"MUNICIPIO",
    #"NUMERO_PREDIAL":"NUMERO_PREDIAL",
    #"TIPO_DE_REGISTRO":"TIPO_DE_REGISTRO",
    "NUMERO_DE_ORDEN":"NUMERO_DE_ORDEN",
    "TOTAL_REGISTROS":"TOTAL_REGISTROS",
    #"NOMBRE":,
    #"ESTADO_CIVIL":,
    #"TIPO_DOCUMENTO",
    #"NUMERO_DOCUMENTO",
    #"DIRECCION",
    #"COMUNA",
    "DESTINO":"DESTINO_ECONOMICO",
    "AREATERR":"AREA_TERRENO",
    "AREACONST":"AREA_CONSTRUIDA",
    "AVALUO":"AVALUO",
    "VIGENCIA":"VIGENCIA",
    "NUMERO_PREDIAL_ANTERIOR":"NUMERO_PREDIAL_ANTERIOR",
    "NPN":"NUMERO_PREDIAL_NACIONAL",
    "ZONA":"ZONA",
    "DERECHO":"DERECHO",
    "FICHA":"FICHA",
    "MATRICULA":"MATRICULA",
    'TIPO_DOC':'TIPO_DOCUMENTO', 
    'DOCUMENTO':'NUMERO_DOCUMENTO'
}


destinos_economicos = {
    # Códigos numéricos
    0: "Na",
    1: "Habitacional",
    2: "Industrial",
    3: "Comercial",
    4: "Agropecuario",
    5: "Mineros_hidrocarburos",
    6: "Cultural",
    7: "Recreacional",
    8: "Salubridad",
    9: "Institucional",
    12: "Lote urbanizado no construido",
    13: "Lote urbanizable no urbanizado",
    14: "Lote no urbanizable",
    19: "Uso publico",
    24: "Agricola",
    25: "Pecuario",
    27: "Educativo",
    28: "Agroindustrial",
    29: "Religioso",
    30: "Forestal",
    60: "Acuicola",
    61: "Agroforestal",
    62: "Infraestructura asociada produccion agropecuaria",
    63: "Infraestructura hidraulica",
    64: "Infraestructura saneamiento basico",
    65: "Infraestructura transporte",
    66: "Servicios funerarios",
    67: "Infraestructura seguridad",
    #Destinos que ya no existen 
     10: "Mixto",
     11:"Otros",
     26:"Servicios Especiales",
     40:"Por Definir Catastro",

    # Códigos alfabéticos
    "A": "Habitacional",
    "AA": "Aa",  # No definido explícitamente
    "AB": "Infraestructura seguridad",
    "AC": "Infraestructura saneamiento basico",
    "B": "Industrial",
    "C": "Comercial",
    "D": "Agropecuario",
    "E": "Mineros_hidrocarburos",
    "F": "Cultural",
    "G": "Recreacional",
    "H": "Salubridad",
    "I": "Institucional",
    "J": "Educativo",
    "K": "Religioso",
    "L": "Agricola",
    "M": "Pecuario",
    "N": "Agroindustrial",
    "O": "Forestal",
    "P": "Uso publico",
    "Q": "Servicios especiales",
    "R": "Lote urbanizable no urbanizado",
    "S": "Lote urbanizado no construido",
    "T": "Lote no urbanizable",
    "U": "Acuicola",
    "V": "Infraestructura hidraulica",
    "W": "Mineros_hidrocarburos",
    "X": "Infraestructura transporte",
    "Y": "Servicios funerarios",
    "Z": "Agroforestal"
}

homologacion_codigos = {
     4: 24,
     15:19,
     16:1,
     17:30,
     20:30,
     21:1,
     22:1,
     23:24,
     31:24
 }


for r1_seleccionado in R1:
    temporal = pd.read_csv(r1_seleccionado, sep="|",on_bad_lines='skip',encoding="latin",header=None)
    Consolidado=pd.concat([Consolidado,temporal])

#print("Columnas finales:")
#print(Consolidado.columns)


ruta_consolidado = BASE_DIR /"Outputs"/str(anio)/"R1_"


if not os.path.exists(ruta_consolidado):
   os.makedirs(ruta_consolidado)
   print("Carpeta creada:", ruta_consolidado)
else:
    print("La carpeta ya existe:", ruta_consolidado)
    
def clean_illegal_chars_series(serie):
    def clean_str(x):
        if isinstance(x, str):
            return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', x)
        return x
    return serie.apply(clean_str)
    

os.chdir(ruta_consolidado) 
Consolidado=Consolidado.reset_index()
Consolidado.drop(columns=["index"],inplace=True)         

Consolidado.columns=campos
Consolidado["Nuevo"]=Consolidado["NPN"].apply(lambda x: str(x)[5:7])

#Revisar
Consolidado.groupby(["Nuevo","SECTOR"])["NPN"].count()


Consolidado.rename(columns=cambio_campos,inplace=True)


Consolidado["ZONA"]=np.where(Consolidado["NUMERO_PREDIAL_NACIONAL"].apply(lambda x: str(x)[5:7])=="00",
"Rural",
"Urbana"
)


Columnas=[col for col in cambio_campos.values() if col in Consolidado.columns]

Consolidado=Consolidado[Columnas]



Consolidado.DESTINO_ECONOMICO.replace(homologacion_codigos,inplace=True)
Consolidado.DESTINO_ECONOMICO.replace(destinos_economicos,inplace=True)
Consolidado.DERECHO=Consolidado.DERECHO/100000000

Consolidado["Filtrar"]=pd.Series([x[0:5] for x in Consolidado.NUMERO_PREDIAL_NACIONAL])

for filtro in np.unique(Consolidado["Filtrar"]):
    Temporal=Consolidado[Consolidado.Filtrar==filtro]
    Temporal.drop(columns=["Filtrar"],inplace=True)
    #Temporal=Temporal[['DEPARTAMENTO', 'MUNICIPIO', 'NUMERO_PREDIAL', 'TIPO_DE_REGISTRO',
    #       'NUMERO_DE_ORDEN', 'TOTAL_REGISTROS', 'NOMBRE', 'ESTADO_CIVIL',
    #       'TIPO_DOCUMENTO', 'NUMERO_DOCUMENTO', 'DIRECCION', 'COMUNA',
    #       'DESTINO_ECONOMICO', 'AREA_TERRENO', 'AREA_CONSTRUIDA', 'AVALUO',
    #       'VIGENCIA', 'NUMERO_PREDIAL_ANTERIOR', 'NUMERO_PREDIAL_NACIONAL']] 
    Temporal.to_excel(f"{filtro}.xlsx", index=False, engine='openpyxl')