"""
Created on Sun Jul 13 19:21:52 2025

@author: danie
"""
from pathlib import Path
import os
import pandas as pd
import zipfile
import numpy as np



BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta relativa al archivo
ruta_input = BASE_DIR /"Inputs"/"Process data"/"Information_2019_2025"/"2021"

os.chdir(ruta_input)



R1_1=pd.read_excel("Registro 1-1 completo.xlsx")
R1_2=pd.read_excel("Registro 1-2 completo.xlsx")

Consolidado=pd.concat([R1_1,R1_2])
 
Consolidado.DEPARTAMENTO=pd.Series([str(0)+str(dep) for dep in Consolidado.DEPARTAMENTO])
Consolidado.MUNICIPIO=pd.Series([str(0)*(3-len(str(dep)))+str(dep) for dep in Consolidado.MUNICIPIO])



Consolidado.NUMERO_PREDIAL_ANTERIOR=np.where((Consolidado.NUMERO_PREDIAL_ANTERIOR==0)|(Consolidado.NUMERO_PREDIAL_ANTERIOR.isna()),
                                             Consolidado.NUMERO_PREDIAL_ANTERIOR.astype(str),
                                             Consolidado.DEPARTAMENTO.astype(str)+Consolidado.MUNICIPIO.astype(str)+Consolidado.NUMERO_PREDIAL_ANTERIOR.astype(str))

Consolidado.NUMERO_PREDIAL_ANTERIOR=Consolidado.NUMERO_PREDIAL_ANTERIOR.astype(str)


# Resumen general
print("Resumen general:")
print(Consolidado.info())

# Primeras filas
print("\nPrimeras 5 filas:")
print(Consolidado.head())

# Descripción estadística (solo numéricas)
print("\nResumen estadístico:")
print(Consolidado.describe())

# Conteo de valores nulos por columna
print("\nValores nulos por columna:")
print(Consolidado.isnull().sum())

# Tipos de datos
print("\nTipos de datos por columna:")
print(Consolidado.dtypes)

# Conteo de valores únicos por columna
print("\nValores únicos por columna:")
print(Consolidado.nunique())

# Si quieres incluir columnas categóricas en describe:
print("\nResumen incluyendo columnas categóricas:")
print(Consolidado.describe(include='all'))


ruta_consolidado = BASE_DIR /"Outputs"/"2021"/"R1"


if not os.path.exists(ruta_consolidado):
   os.makedirs(ruta_consolidado)
   print("Carpeta creada:", ruta_consolidado)
else:
    print("La carpeta ya existe:", ruta_consolidado)
    
    
    

os.chdir(ruta_consolidado) 
Consolidado["Filtrar"]=Consolidado.DEPARTAMENTO.astype(str)+Consolidado.MUNICIPIO.astype(str)    
Consolidado.rename(columns={'NUMERO_PREDIAL ':'NUMERO_PREDIAL'},inplace=True)


cambio_campos = {
    #"DEPARTAMENTO":"DEPARTAMENTO",
    'MUNICIPIO':"MUNICIPIO",
    #"NUMERO_PREDIAL":"NUMERO_PREDIAL",
    #"TIPO_DE_REGISTRO":"TIPO_DE_REGISTRO",
    'NUMERO_DE_ORDEN':"NUMERO_DE_ORDEN",
    'TOTAL_REGISTROS':"TOTAL_REGISTROS",
    #"NOMBRE":,
    #"ESTADO_CIVIL":,
    #"TIPO_DOCUMENTO",
    #"NUMERO_DOCUMENTO",
    #"DIRECCION",
    #"COMUNA",
    'DESTINO_ECONOMICO':"DESTINO_ECONOMICO",
    'AREA_TERRENO':"AREA_TERRENO",
    'AREA_CONSTRUIDA':"AREA_CONSTRUIDA",
    'AVALUO':"AVALUO",
    "VIGENCIA":"VIGENCIA",
    'NUMERO_PREDIAL_ANTERIOR':"NUMERO_PREDIAL_ANTERIOR",
    'NUMERO_PREDIAL_NACIONAL':"NUMERO_PREDIAL_NACIONAL",
    "ZONA":"ZONA",
    "DERECHO":"DERECHO",
    "FICHA":"FICHA",
    "MATRICULA":"MATRICULA",
    'TIPO_DOCUMENTO':'TIPO_DOCUMENTO', 
    'NUMERO_DOCUMENTO':'NUMERO_DOCUMENTO'
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


Consolidado["ZONA"]=np.where(Consolidado["NUMERO_PREDIAL_NACIONAL"].apply(lambda x: str(x)[5:7])=="00",
"Rural",
"Urbana"
)
Columnas=[col for col in cambio_campos if col in Consolidado.columns]
Consolidado=Consolidado[Columnas]
Consolidado.DESTINO_ECONOMICO.replace(homologacion_codigos,inplace=True)
Consolidado.DESTINO_ECONOMICO.replace(destinos_economicos,inplace=True)

os.chdir(ruta_consolidado) 
Consolidado["Filtrar"]=Consolidado["NUMERO_PREDIAL_NACIONAL"].astype(str).apply(lambda x: str(x)[0:5]) 


for filtro in np.unique(Consolidado["Filtrar"]):
    Temporal=Consolidado[Consolidado.Filtrar==filtro]
    Temporal=Temporal=Temporal[Columnas]  
    Temporal.to_excel(f"{filtro}.xlsx", index=False, engine='openpyxl')
    
 
    