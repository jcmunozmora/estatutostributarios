library(readxl)
library(stringr)
library(dplyr)
library(purrr)
library(tidyr)
library(readr)
library(writexl)

facturacion <- function(folder_path,
                        sheet = NULL,
                        year_min = 1990,
                        year_max = 2100,
                        # llaves de unión
                        keys = c("cedula","ficha"),
                        # carpeta de salida
                        output_dir = "03_Outputs",
                        # exportar también la versión larga/apilada
                        export_all_long = TRUE) {
  # 1) Archivos y años --------------------------------------------------------
  files <- list.files(folder_path, pattern = "\\.(xls|xlsx)$",
                      full.names = TRUE, ignore.case = TRUE)
  if (length(files) == 0) stop("No se encontraron archivos .xls/.xlsx en la carpeta.")
  file_names <- basename(files)
  years_raw  <- str_extract(file_names, "\\b\\d{4}\\b")
  years_num  <- suppressWarnings(as.integer(years_raw))
  
  valid_idx  <- !is.na(years_num) & years_num >= year_min & years_num <= year_max
  files      <- files[valid_idx]
  file_names <- file_names[valid_idx]
  years_num  <- years_num[valid_idx]
  if (length(files) == 0) stop("No se encontraron archivos con año válido en el nombre.")
  
  # prioridad descendente (p.ej. 2025 primero)
  years_unique <- sort(unique(years_num), decreasing = TRUE)
  
  # 2) Leer por año (lista) ---------------------------------------------------
  data_by_year <- map(years_unique, function(yr) {
    idx_yr <- which(years_num == yr)
    map_dfr(idx_yr, function(i) {
      df <- read_excel(files[i], sheet = sheet)
      
      # Renombrar 2025 (ajusta si cambian encabezados)
      if (yr == 2025) {
        df <- df %>% rename(
          cedula                = `DOCUMENTO`,
          ficha                 = `NÚMERO DE FICHA`,
          avaluo                = `AVALÚO`,
          tarifa                = `TARIFA PREDIAL`,
          exento                = `EXENTO`,
          Liq_2024              = `TOTAL LIQUIDACION IMPUESTO PREDIAL VIGENCIA 2024`,
          Liq_2025              = `TOTAL LIQUIDACION IMPUESTO PREDIAL VIGENCIA 2025`,
          `VALOR IMPUESTO PREDIAL VIGENCIAS ANTERIORES O CON MORA` =
            `VALOR IMPUESTO PREDIAL VIGENCIAS ANTERIORES O CON MORA`,
          int_mora              = `INTERESES DE MORA`,
          ult_trimest_cancelado = `ULTIMO TRIMESTRE CANCELADO`
        )
      }
      
      # Claves como texto + trim
      df <- df %>%
        mutate(across(any_of(c("cedula","ficha","matricula","nup","cons_pred")), as.character)) %>%
        mutate(across(where(is.character), stringr::str_trim))
      
      # --- dentro de cada archivo/año (conservar primera por llave) ---
      df <- df %>% distinct(across(all_of(keys)), .keep_all = TRUE)
      
      df %>% mutate(year = yr, source = file_names[i])
    })
  })
  names(data_by_year) <- as.character(years_unique)
  
  # 3) Recalcular ALL DATA (largo/apilado) -----------------------------------
  data_all <- bind_rows(data_by_year)
  
  # Validar que estén las llaves
  cols_all <- names(data_all)
  keys_missing <- setdiff(keys, cols_all)
  if (length(keys_missing) > 0) {
    stop(paste0("Faltan llaves en la data: ", paste(keys_missing, collapse = ", "),
                ". Ajusta 'keys' o revisa el rename."))
  }
  
  # --- en el largo por (keys + year) ---
  data_all <- data_all %>% distinct(across(all_of(c(keys, "year"))), .keep_all = TRUE)
  
  # 4) Construir WIDE con sufijo _YYYY (LEFT JOIN desde el último año) -------
  cols_excluir <- c("source")
  df_wide_list <- map(years_unique, function(anio){
    dfi <- data_by_year[[as.character(anio)]]
    medidas <- setdiff(names(dfi), c(keys, "year", cols_excluir))
    dfi %>%
      select(any_of(c(keys, medidas))) %>%
      rename_with(~ paste0(.x, "_", anio), all_of(medidas)) %>%
      distinct()
  })
  names(df_wide_list) <- as.character(years_unique)
  
  data_wide <- df_wide_list[[as.character(years_unique[1])]]      # p.ej. 2025 primero
  if (length(df_wide_list) > 1) {
    for (k in 2:length(df_wide_list)) {
      data_wide <- left_join(data_wide, df_wide_list[[k]], by = keys)
    }
  }
  
  # 5) Reporte de presencia por año ------------------------------------------
  presence <- data_all %>%
    distinct(across(all_of(c(keys, "year")))) %>%
    mutate(presente = 1) %>%
    pivot_wider(names_from = year, values_from = presente,
                values_fill = 0, names_prefix = "has_") %>%
    mutate(n_present = rowSums(across(starts_with("has_"))))
  
  # Pegar flags al WIDE y ordenar columnas
  data_wide <- left_join(data_wide, presence, by = keys)
  all_cols   <- names(data_wide)
  flag_cols  <- c(grep("^has_\\d{4}$", all_cols, value = TRUE), "n_present")
  year_cols  <- unlist(lapply(years_unique, function(y) grep(paste0("_", y, "$"), all_cols, value = TRUE)))
  other_cols <- setdiff(all_cols, c(keys, flag_cols, year_cols))
  data_wide  <- data_wide %>% select(any_of(c(keys, flag_cols, year_cols, other_cols)))
  
  # --- Duplicados ---
  data_wide <- data_wide %>% distinct(across(all_of(keys)), .keep_all = TRUE)
  
  # 6) SUBSET  -----------------
  subset_vars <- c(
    keys,
    flag_cols,
    paste0("avaluo_", c(2025, 2024, 2023, 2022)),
    paste0("tarifa_", c(2025, 2024, 2023, 2022)),
    "Liq_2024_2025", "Liq_2025_2025", "int_mora_2025",
    paste0("estrato_", c(2024, 2023, 2022)),
    paste0("vlr_predial_", c(2024, 2023, 2022))
  )
  data_subset <- data_wide %>% select(any_of(subset_vars))
  
  # 7) Exportar ---------------------------------------------------------
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  write_csv(data_wide,    file.path(output_dir, "05321_facturacion_wide.csv"))
  write_csv(presence,     file.path(output_dir, "05321_facturacion_merge_report.csv"))
  writexl::write_xlsx(data_subset, file.path(output_dir, "05321_facturacion_subset.xlsx"))
  saveRDS(data_subset,                file.path(output_dir, "05321_facturacion_subset.rds"))
  
  if (export_all_long) {
    write_csv(data_all,  file.path(output_dir, "05321_facturacion_long.csv"))
    saveRDS(data_all,    file.path(output_dir, "05321_facturacion_long.rds"))
  }
  
  # 8) Output ---------------------------------------------------------------
  list(
    years = years_unique,
    data_by_year = data_by_year,
    data_all = data_all,
    data_wide = data_wide,
    merge_report = presence,
    data_subset = data_subset
  )
}