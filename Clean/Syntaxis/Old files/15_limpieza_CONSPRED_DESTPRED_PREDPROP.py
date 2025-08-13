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
         'DESTPRED':"utf-8",##Lo guardo
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





Consolidado_CONSPRED = load_files(variable='CONSPRED',encoding1=limpios['CONSPRED'])

Consolidado_CONSPRED["CONS_AREA"] = pd.to_numeric(
    Consolidado_CONSPRED["CONS_AREA"].str.replace(",", ".", regex=False),
    errors="coerce"
)
CONSPRED_guardar=Consolidado_CONSPRED.groupby(["CONS_NRO_FICHA",'ANIO']).agg({"CONS_AREA": "sum", "CONS_VALOR": "sum"}).reset_index()



Consolidado_DESTPRED = load_files(variable='DESTPRED',encoding1=limpios['DESTPRED'])
Consolidado_DESTPRED.sort_values(["ANIO","DEPR_NRO_FICHA", "DEPR_DEST_ECON"],inplace=True)
Consolidado_DESTPRED=Consolidado_DESTPRED.reset_index().drop(columns=["index"])
Consolidado_DESTPRED["Consecutivo"] = (
    Consolidado_DESTPRED
    .groupby(["DEPR_NRO_FICHA", "ANIO"])
    .cumcount() + 1
)
Consolidado_DESTPRED["EtiquetaDestino"] = "Destino " + Consolidado_DESTPRED["Consecutivo"].astype(str)
Consolidado_DESTPRED["EtiquetaPorcentaje_Destino"] = "Porc. Destino " + Consolidado_DESTPRED["Consecutivo"].astype(str)
Temporal_1_DESTPRED=Consolidado_DESTPRED.pivot(index=["DEPR_NRO_FICHA","ANIO"], columns="EtiquetaDestino", values='DEPR_DEST_ECON').reset_index()
Temporal_2_DESTPRED=Consolidado_DESTPRED.pivot(index=["DEPR_NRO_FICHA","ANIO"], columns="EtiquetaPorcentaje_Destino", values='DEPR_PORC_ECON').reset_index()
DESTPRED_guardar=Temporal_1_DESTPRED.merge(Temporal_2_DESTPRED,on=['DEPR_NRO_FICHA', 'ANIO'],how="inner")





Consolidado_PREDPROP = load_files(variable='PREDPROP',encoding1=limpios['PREDPROP'])

Consolidado_PREDPROP=Consolidado_PREDPROP[["PRPO_DOCUMENTO","PRPO_DERECHO","PRPO_MATRICULA_INM","PRPO_NRO_FICHA","ANIO"]]


salida = BASE_DIR /"Inputs"/"Route Data"/"2005-2011"

for ANIO1 in list(range(2006,2012)):
    DESTPRED_guardar[DESTPRED_guardar.ANIO==ANIO1].drop(columns=["ANIO"]).to_csv(salida/f"DESTPRED_{ANIO1}.txt", index=False, sep="|", encoding="utf-8")
    Consolidado_PREDPROP[Consolidado_PREDPROP.ANIO==ANIO1].drop(columns=["ANIO"]).to_csv(salida/f"PREDPROP_{ANIO1}.txt", index=False, sep="|", encoding="utf-8")
    CONSPRED_guardar[CONSPRED_guardar.ANIO==ANIO1].drop(columns=["ANIO"]).to_csv(salida/f"CONSPRED_{ANIO1}.txt", index=False, sep="|", encoding="utf-8")



