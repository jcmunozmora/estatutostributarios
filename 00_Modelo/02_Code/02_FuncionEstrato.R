# Funcion Estratos
library(dplyr)
library(stringr)

estrato_from_long <- function(muni,
                              input_dir  = "01_Data/Derived",
                              output_dir = input_dir) {
  muni <- as.character(muni)
  
  in_path <- file.path(input_dir, paste0(muni, "_facturacion_long.rds"))
  if (!file.exists(in_path)) {
    stop("No encontré el archivo: ", in_path)
  }
  
  # 1) Leer long (RDS)
  df <- readRDS(in_path)
  n_in <- nrow(df)
  
  # 2) Filtrar por cedula, ficha, estrato (+ year si existe)
  keep <- c("cedula", "ficha", "estrato", "year")
  df <- df %>%
    dplyr::select(dplyr::any_of(keep)) %>%
    mutate(
      cedula = as.character(cedula),
      ficha  = as.character(ficha),
      estrato = if ("estrato" %in% names(.)) estrato else NA
    ) %>%
    mutate(across(c(cedula, ficha), ~ str_trim(.)))
  
  # 3) Resolver duplicados: preferir year más reciente 
  if ("year" %in% names(df)) {
    df_out <- df %>%
      arrange(desc(year), !is.na(estrato)) %>%   # primero año mayor; luego estrato no-NA
      distinct(cedula, ficha, .keep_all = TRUE) %>%
      select(cedula, ficha, estrato)
  } else {
    df_out <- df %>%
      distinct(cedula, ficha, .keep_all = TRUE) %>%
      select(cedula, ficha, estrato)
  }
  
  # 4) Exportar RDS como "<muni> estrato.rds"
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  out_path <- file.path(output_dir, paste0(muni, " estrato.rds"))
  saveRDS(df_out, out_path)
  
  # 5) Output 
  list(
    input_file  = in_path,
    output_file = out_path,
    n_in_rows   = n_in,
    n_out_pairs = nrow(df_out),
    data        = df_out
  )
}

