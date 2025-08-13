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
import pickle

BASE_DIR = Path(__file__).resolve().parent.parent
Anio=2011
# Ruta relativa al archivo
ruta_input = BASE_DIR /"Inputs"/"Process data"/"2006-2011"

os.chdir(ruta_input)

Archivos=os.listdir()

limpios =np.unique([re.sub(r'(_\d{4})?\.csv$', '', f) for f in Archivos])
# Crear DataFrame vacío

limpios={'CONSPRED':"utf-8",
         'DESTPRED':"utf-8",
         'PREDFORM':"latin1",
         'PREDPROP':"latin1",
         'PROPIETA':"latin1",
         'ZOGEMPIO':"latin1",
         'ZOGEPRED':"latin1",
         'ZONAGEOC':"latin1"}

# Leer y concatenar cada archivo
def load_files(variable,encoding1):
    Consolidado = pd.DataFrame()
    i = 0
    ARCHIVOS= [f for f in os.listdir() if f.startswith(variable)]
    for archivo_ in ARCHIVOS:
        try:
            temporal = pd.read_csv(archivo_, sep=";",on_bad_lines='skip',encoding=encoding1)
        except:
            try:
                temporal = pd.read_csv(archivo_, sep=",",on_bad_lines='skip',encoding=encoding1)
            except:
                try:
                    temporal = pd.read_csv(archivo_, sep="|",on_bad_lines='skip',encoding=encoding1)
                except:
                    temporal = pd.read_csv(archivo_, sep="\t",on_bad_lines='skip',encoding=encoding1)
        # Si la primera columna tiene todas las cabeceras unidas, separar por el separador detectado
        anio = int(re.sub(r'.*_(\d{4})\.csv$', r'\1', archivo_))
        
        if len(temporal.columns) == 1:
            sep_detectado = None
            for sep in [';', ',', '|', '\t']:
                if sep in temporal.columns[0]:
                    sep_detectado = sep
                    break
            if sep_detectado:
                temporal = pd.read_csv(archivo_, sep=sep_detectado, on_bad_lines='skip',encoding=encoding1)
        temporal["ANIO"]=anio
        # Concatenar
        Consolidado = pd.concat([Consolidado, temporal], ignore_index=True)
        print(f"Archivo procesado: {i}")
        i += 1
    return Consolidado     

for var in limpios.keys():
    if pd.notna(var):  # verifica que no sea nulo
        globals()[f"Consolidado_{var}"] = load_files(variable=var,encoding1=limpios[var])

print("Revisión nombres Columnas")
for var in limpios.keys():
    if pd.notna(var):
        print(f"Nombre:{var}")
        print("-------------")
        print(globals()[f"Consolidado_{var}"].columns)
        print("-------------")
        print("\n")
        print("\n")
        print("\n")

i=0
print("Revisión Llaves")
for var in limpios.keys():
    if pd.notna(var):
        if i==0:
            ID= {var:f for f in globals()[f"Consolidado_{var}"].columns if f.endswith("NRO_FICHA")}
        else:
            ID= ID|{var:f for f in globals()[f"Consolidado_{var}"].columns if f.endswith("NRO_FICHA")}
        i+=1
print(ID)

print("Revisión unicidad de las llaves")
Llaves_unicas={}
for var in ID.keys():
     Temporal=globals()[f"Consolidado_{var}"][[ID[var],"ANIO"]]
     Llaves_unicas=Llaves_unicas|{var:[Temporal.shape[0]==Temporal.drop_duplicates().shape[0],Temporal.shape[0],Temporal.drop_duplicates().shape[0]]}
print(Llaves_unicas)    


print("Combinación de variables como posible llave")
Posibles_llaves = {}
Control_acceso=[]
for var in ID.keys():
    if var!='PREDFORM':
        Temporal = globals()[f"Consolidado_{var}"]
        COLUMNAS = Temporal.columns
        for col1 in COLUMNAS:
            Filtro = np.unique([ID[var], "ANIO", col1])  
            Temporal1 = Temporal[Filtro]
            # Verificar si combinación es llave
            if Temporal1.shape[0] == Temporal1.drop_duplicates().shape[0]:
                if var not in Control_acceso:
                   Posibles_llaves[var] =list(Filtro)
                   Control_acceso.append(var)
                else:
                   Posibles_llaves[var]=Posibles_llaves[var]+list(Filtro) 
    
Posibles_llaves['ZOGEPRED']=['ANIO', 'ZGPR_ZONA_GE','ZGPR_NRO_FICHA']                
print(Posibles_llaves)     
print("Posibles variables que cruzan")
Posibles_cruces={}
Columnas=list(Consolidado_PREDFORM.columns)
Columnas= [campo for campo in Columnas if not re.search(r"(ANIO|PRED_NRO_FICHA)", campo)]
listas_tres = [['ANIO', col, 'PRED_NRO_FICHA'] for col in Columnas]
for var,filtradas in Posibles_llaves.items():
    Temporal = globals()[f"Consolidado_{var}"][filtradas]
    Temporal_tuplas = list(Temporal.itertuples(index=False, name=None))
    for col1 in listas_tres :
        Temporal_tuplas1=list(Consolidado_PREDFORM[col1].itertuples(index=False, name=None))
        comunes = set(Temporal_tuplas) & set(Temporal_tuplas1)
        porcentaje = len(comunes) / len(set(Temporal_tuplas1)) * 100
        Valor_exp=col1[1]
        clave = f"{var}_{Valor_exp}_{col1}"
        Posibles_cruces[clave] = porcentaje

print("Revisión Llaves")
print(ID)
print("Revisión unicidad de las llaves")
print(Llaves_unicas)
print("Combinación de variables como posible llave")
print(Posibles_llaves)
print("Posibles variables que cruzan")
print(Posibles_cruces)



# Guardar como archivo pickle

os.chdir(BASE_DIR /"Inputs"/"Route Data"/"2005-2011")
with open('Revision_Llaves.pkl', 'wb') as f:
    pickle.dump(ID, f)

with open('Llaves_unicas.pkl', 'wb') as f:
    pickle.dump(Llaves_unicas, f)


with open('Combinacion_llave.pkl', 'wb') as f:
    pickle.dump(Posibles_llaves, f)

with open('Posibles_cruces.pkl', 'wb') as f:
    pickle.dump(Posibles_cruces, f)

