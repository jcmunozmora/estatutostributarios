rm(list = ls())
# SOURCES
source("02_Code/01_FuncionFacturacion.R") 
source("02_Code/02_FuncionEstrato.R")
source("02_Code/03_FuncionGIS.R")
source("02_Code/04_R1Pro.R")
source("02_Code/05_Estatutos.R")
source("02_Code/06_EscenarioEstatuto.R")
source("02_Code/07_Quintiles.R")
source("02_Code/08_ATarifas.R")
source("02_Code/09_Escenarios.R")

# Funcion 1
# BASE DE DATOS DE FACTURACION
ruta <- "01_Data/Org/Facturacion/053840"
res <- facturacion(
  folder_path = ruta,
  keys = c("cedula","ficha"),
  output_dir = "01_Data/Derived",
  export_all_long = TRUE  
)
facturacion <- res$data_subset

# Funcion 2
# Estrato Socioeconomico
res <- estrato_from_long(
  muni = "05321",                  
  input_dir  = "01_Data/Derived", 
  output_dir = "01_Data/Derived"   
)
estrato <- res$data
# Funcion 3
# GIS
GIS <- conflictos("01_Data/Org/Conflictos/05321/Conflictos.xlsx")
# Funcion 4 
# R1 PRO
res <- r1_pro(
  muni = "05321",
  year = 2024,
  parquet_root = "01_Data/Org/R1",
  derived_root = "01_Data/Derived",
  output_dir   = "01_Data/Derived"
)
r1 <- res$data_subset
rm(res)
# Funcion 5 
# ESTATUTOS
es_gtpe <- estatuto(
  path_xlsx = "01_Data/Org/Plantilla_EstatutosTributarios.xlsx",
  municipio = "Guatapé",               
  sheet = "BD_Transcripciones"
)

# GUATAPE 
# Escenario 1 Tarifas 
r1_estatuto <- asignar_tarifa(r1, es_gtpe)
sum(r1_estatuto$avaluo_cal, na.rm = TRUE)

# Escenarios
esc <- generar_escenarios_powerset(
  r1 ,
  avaluo_var = "AVALUO",
  area_var   = "AREA_TERRENO",
  output_rda = "01_Data/Derived/escenarios_powerset.rda",
  object_name = "escenarios_r1"
)

# Asignacion de tarifas
# Aleatorio
esc_1 <- asignar_tarifa_escenario(esc, id_escenario = 5, seed = 2024)
# LOGICO
esc_1 <- asignar_tarifa_escenario(esc, 20, seed = 999, tarifas = c(1, 2,3,4))

# FUNCION FINAL - AGREGAR A R1 NUEVA TARIFA 
r1_con_tarifa <- escenarios_reglas(
  r1,
  esc_filtrado = esc_1,
  col_avaluo = "AVALUO",
  col_area   = "AREA_TERRENO",
  tarifa_base = "permil"
)



