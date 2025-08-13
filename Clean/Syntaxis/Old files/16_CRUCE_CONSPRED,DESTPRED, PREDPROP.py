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

os.chdir(BASE_DIR/"Syntaxis")

from Funciones_Compartidas import  load_files

ruta_input = BASE_DIR /"Inputs"/"Route Data"/"2005-2011"

os.chdir(ruta_input)




Consolidado_CONSPRED = load_files(variable='CONSPRED',encoding1="utf-8")
Consolidado_DESTPRED = load_files(variable='DESTPRED',encoding1="utf-8")
Consolidado_PREDPROP = load_files(variable='PREDPROP',encoding1="latin1")

ruta_input = BASE_DIR /"Inputs"/"Process data"/"2006-2011"
os.chdir(ruta_input)
Consolidado_PREDFORM = load_files(variable='PREDFORM',encoding1="latin1")



print("Fichas: que no están en la base PREDFORM")
print(sum(~Consolidado_DESTPRED["DEPR_NRO_FICHA"].isin(Consolidado_PREDFORM['PRED_NRO_FICHA'])))

Concatenado=Consolidado_PREDFORM.merge(Consolidado_DESTPRED, left_on=['PRED_NRO_FICHA',"ANIO"],
    right_on=["DEPR_NRO_FICHA","ANIO"],how="left")


print("Fichas: que no están en la base PREDFORM")
print(sum(~Consolidado_PREDPROP['PRPO_NRO_FICHA'].isin(Consolidado_PREDFORM['PRED_NRO_FICHA'])))


Concatenado=Concatenado.merge(Consolidado_PREDPROP, left_on=['PRED_NRO_FICHA',"ANIO"],
    right_on=['PRPO_NRO_FICHA',"ANIO"],how="left")

Concatenado.sort_values(["ANIO",'PRED_NRO_FICHA','PRPO_DOCUMENTO'],inplace=True)
Concatenado=Concatenado.reset_index().drop(columns=["index"])
Concatenado["Consecutivo"] = (
   Concatenado
    .groupby(['PRED_NRO_FICHA', "ANIO"])
    .cumcount() + 1
)



pd.unique(Concatenado['PRED_MPIO'])


Concatenado[((Concatenado['PRED_MPIO']==2)&(Concatenado.ANIO==2011))].to_excel(BASE_DIR/"Temporal.xlsx")

