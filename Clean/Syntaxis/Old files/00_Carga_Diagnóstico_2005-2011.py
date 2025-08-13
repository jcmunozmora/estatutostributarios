# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 19:07:31 2025

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

Llaves={'CONSPRED': 'CONS_NRO_FICHA',
 'DESTPRED': 'DEPR_NRO_FICHA',
 'PREDPROP': 'PRPO_NRO_FICHA',
 'ZOGEPRED': 'ZGPR_NRO_FICHA'}

os.chdir(ruta_input)
def Revisar_bases(Vari,encoding2,ANIO1,llave):
    Consolidado=load_files(variable=Vari,encoding1=encoding2)
    revisar = Consolidado.groupby([llave, "ANIO"]).size().reset_index(name="cuenta")
    fichas_max = set(revisar.loc[revisar.cuenta == revisar.cuenta.max(), llave])
    filtro = Consolidado[llave].isin(fichas_max)
    resultado = Consolidado[filtro]
    resultado=resultado[resultado.ANIO==ANIO1]
    resultado.to_excel(BASE_DIR/f"Revisar_rep_{Vari}.xlsx")


for Vari1 in Llaves.keys():
    Revisar_bases(Vari=Vari1,encoding2=limpios[Vari1],ANIO1=2011,llave=Llaves[Vari1])



os.chdir(BASE_DIR /"Inputs"/"Route Data"/"2005-2011")


with open('Revision_Llaves.pkl', 'rb') as f:
    Revision_Llaves = pickle.load(f)

with open('Llaves_unicas.pkl', 'rb') as f:
    Llaves_unicas = pickle.load(f)


with open('Combinacion_llave.pkl', 'rb') as f:
    Combinacion_llave = pickle.load(f)

with open('Posibles_cruces.pkl', 'rb') as f:
    Posibles_cruces = pickle.load(f)
