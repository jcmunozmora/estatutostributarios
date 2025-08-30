# PAQUETES:
library(arrow)
library(dplyr)
library(stringr)
library(readr)
library(tibble)

r1_pro <- function(muni,
                   year         = 2024,
                   parquet_root = "01_Data/Org/R1",
                   derived_root = "01_Data/Derived",
                   output_dir   = "01_Data/Derived") {
  
  muni <- as.character(muni)
  year <- as.integer(year)
  
  # ---- 
  parquet_path   <- file.path(parquet_root, muni, year, paste0(muni, ".parquet"))
  long_path      <- file.path(derived_root, paste0(muni, "_facturacion_subset.rds"))
  estrato_path   <- file.path(derived_root, paste0(muni, " estrato.rds"))
  conflictos_rda <- file.path(derived_root, paste0(muni, "_conflictos.rda"))  # <- NUEVO
  
  if (!file.exists(parquet_path)) stop("No encuentro el Parquet: ", parquet_path)
  if (!file.exists(long_path))    stop("No encuentro el long RDS: ", long_path)
  if (!file.exists(estrato_path)) stop("No encuentro el estrato RDS: ", estrato_path)
  
  # ---- 1) Base principal (R1)
  pq <- arrow::read_parquet(parquet_path) |> as_tibble()
  
  nsp <- names(pq)
  ficha_col <- nsp[str_detect(nsp, regex("^\\s*ficha\\s*$", ignore_case = TRUE))]
  if (length(ficha_col) == 0)
    ficha_col <- nsp[str_detect(nsp, regex("numero\\s*de\\s*ficha|^ficha$", ignore_case = TRUE))]
  if (length(ficha_col) == 0)
    ficha_col <- nsp[str_detect(nsp, regex("^\\s*FICHA\\s*$", ignore_case = TRUE))]
  
  doc_col <- nsp[str_detect(nsp, regex("^\\s*numero_?documento\\s*$", ignore_case = TRUE))]
  if (length(doc_col) == 0)
    doc_col <- nsp[str_detect(nsp, regex("^\\s*documento\\s*$|^num_?doc$|^cedula$", ignore_case = TRUE))]
  
  if (length(ficha_col) == 0 || length(doc_col) == 0) {
    stop("La base principal no tiene llaves reconocibles (FICHA / NUMERO_DOCUMENTO).")
  }
  
  pq <- pq |>
    mutate(
      !!ficha_col[1] := as.character(.data[[ficha_col[1]]]),
      !!doc_col[1]   := as.character(.data[[doc_col[1]]])
    ) |>
    rename(FICHA = all_of(ficha_col[1]),
           NUMERO_DOCUMENTO = all_of(doc_col[1])) |>
    mutate(across(c(FICHA, NUMERO_DOCUMENTO), str_trim))
  
  # ---- 2) facturacion_long
  long_df <- readRDS(long_path)
  if ("cedula" %in% names(long_df)) names(long_df)[names(long_df)=="cedula"] <- "NUMERO_DOCUMENTO"
  if ("CEDULA" %in% names(long_df)) names(long_df)[names(long_df)=="CEDULA"] <- "NUMERO_DOCUMENTO"
  if ("ficha"  %in% names(long_df)) names(long_df)[names(long_df)=="ficha"]  <- "FICHA"
  long_df <- long_df |>
    mutate(
      NUMERO_DOCUMENTO = as.character(NUMERO_DOCUMENTO),
      FICHA            = as.character(FICHA)
    ) |>
    mutate(across(c(NUMERO_DOCUMENTO, FICHA), str_trim))
  
  # ---- 3) estrato
  es <- readRDS(estrato_path)
  if ("cedula" %in% names(es)) names(es)[names(es)=="cedula"] <- "NUMERO_DOCUMENTO"
  if ("CEDULA" %in% names(es)) names(es)[names(es)=="CEDULA"] <- "NUMERO_DOCUMENTO"
  if ("ficha"  %in% names(es)) names(es)[names(es)=="ficha"]  <- "FICHA"
  es <- es |>
    mutate(
      NUMERO_DOCUMENTO = as.character(NUMERO_DOCUMENTO),
      FICHA            = as.character(FICHA)
    ) |>
    mutate(across(c(NUMERO_DOCUMENTO, FICHA), str_trim))
  
  # ---- 4) JOINs base + long + estrato
  joined1 <- left_join(pq, long_df, by = c("NUMERO_DOCUMENTO", "FICHA"))
  long_cols_added <- setdiff(names(joined1), names(pq))
  has_long <- if (length(long_cols_added) == 0) rep(FALSE, nrow(joined1))
  else rowSums(!is.na(joined1[, long_cols_added, drop = FALSE])) > 0
  
  joined2 <- left_join(joined1, es, by = c("NUMERO_DOCUMENTO", "FICHA"))
  has_estrato <- "estrato" %in% names(joined2) & !is.na(joined2$estrato)
  
  # ---- 5) NUM_PREDIOS y categoría
  joined2 <- joined2 %>%
    mutate(NUMERO_DOCUMENTO = as.character(NUMERO_DOCUMENTO)) %>%
    group_by(NUMERO_DOCUMENTO) %>%
    mutate(NUM_PREDIOS = n()) %>%
    ungroup() %>%
    mutate(CAT_NUMERO_PREDIOS = ifelse(NUM_PREDIOS == 1, "uno", "uno o mas"))
  

  # ---- 6) JOIN con CONFLICTOS por PK_PREDIOS 
  joined3 <- joined2
  if (file.exists(conflictos_rda)) {
    envc <- new.env(parent = emptyenv())
    objs <- load(conflictos_rda, envir = envc)
    conf <- if ("conflictos" %in% objs) envc$conflictos else {
      df_names <- objs[vapply(objs, function(nm) is.data.frame(envc[[nm]]), logical(1))]
      if (!length(df_names)) stop("El RDA de conflictos no contiene data.frames.")
      envc[[df_names[1]]]
    }
    conf <- tibble::as_tibble(conf)
    
    if (!"PK_PREDIOS" %in% names(conf))    stop("Conflictos NO tiene 'PK_PREDIOS'.")
    if (!"PK_PREDIOS" %in% names(joined2)) stop("Base principal NO tiene 'PK_PREDIOS'.")
    
    # normalizar tipos/espacios 
    conf   <- conf   %>% mutate(PK_PREDIOS = as.character(str_trim(PK_PREDIOS)))
    joined2 <- joined2 %>% mutate(PK_PREDIOS = as.character(str_trim(PK_PREDIOS)))
    dups <- sum(duplicated(conf$PK_PREDIOS))
    if (dups > 0) warning("Conflictos trae ", dups, " PK_PREDIOS duplicados; el join puede multiplicar filas.")
    
    joined3 <- left_join(joined2, conf, by = "PK_PREDIOS", suffix = c("", "_conf"))
  } else {
    warning("No encontré conflictos RDA: ", conflictos_rda, " (sigo sin anexar conflictos).")
  }

  
  # ---- 7) Informe
  n0 <- nrow(pq)
  n_long    <- sum(has_long,    na.rm = TRUE)
  n_estrato <- sum(has_estrato, na.rm = TRUE)
  report <- tibble(
    municipio          = muni,
    year               = year,
    filas_base         = n0,
    filas_con_long     = n_long,
    tasa_long          = ifelse(n0 > 0, n_long / n0, NA_real_),
    filas_con_estrato  = n_estrato,
    tasa_estrato       = ifelse(n0 > 0, n_estrato / n0, NA_real_)
  )
  
  # ---- 8) Subset (igual que tenías)
  cols_keep <- c(
    "MUNICIPIO",
    "FICHA",
    "DESTINO_ECONOMICO",
    "ZONA",
    "AREA_TERRENO",
    "AREA_CONSTRUIDA",
    "AVALUO_CONSTRUCCION",
    "AVALUO_TERRENO",
    "AVALUO",
    "DERECHO",
    "GRAVABLE",
    "AUTOESTIMACION",
    "Liq_2024_2025",
    "Liq_2025_2025",
    "int_mora_2025",
    "estrato",
    "NUM_PREDIOS",
    "CAT_NUMERO_PREDIOS",
    "%Adecuado",
    "%Sobreutilización", 
    "%Subutilización",
    "%Usos sin conflicto",
    "Conflicto"
  )
  R1_pro <- joined3 %>% select(any_of(cols_keep))
  
  # ---- 9) Variables
  R1_pro <- R1_pro %>%
    mutate(adecuado = ifelse(!is.na(`%Adecuado`) & `%Adecuado` > 0, 1L, 0L),
           sobreutilizado = ifelse(!is.na(`%Sobreutilización`) & `%Sobreutilización` > 0, 1L, 0L),
           sobreutilizado = ifelse(!is.na(`%Subutilización`) & `%Subutilización` > 0, 1L, 0L),
           noconflicto = ifelse(!is.na(`%Usos sin conflicto`) & `%Usos sin conflicto` > 0, 1L, 0L),
           deudor = ifelse(!is.na(`int_mora_2025`) & `int_mora_2025` > 0, 1L, 0L) 
           )
  R1_pro <- R1_pro %>%
    mutate(
      nro_predios = case_when(
        (estrato == 6 | estrato == 5 | estrato == 4) & (NUM_PREDIOS > 1) ~ "1 o mas",
        (estrato == 1 | estrato == 2 | estrato == 3) & (NUM_PREDIOS == 1) ~ "1.0",
        (estrato == 1 | estrato == 2 | estrato == 3) & (NUM_PREDIOS > 1) ~ "2 o mas"
      )
    )
  
  
  # ---- 9) Exportar
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  out_csv <- file.path(output_dir, paste0(muni, "_r1_pro.csv"))
  out_rds <- file.path(output_dir, paste0(muni, "_r1_pro.rds"))
  
  readr::write_csv(joined3, out_csv)
  saveRDS(joined3, out_rds)
  
  readr::write_csv(R1_pro, file.path(output_dir, paste0(muni, "_R1_pro_subset.csv")))
  saveRDS(R1_pro, file.path(output_dir, paste0(muni, "_R1_pro_subset.rds")))
  
  # ---- 10) Salida
  list(
    report      = report,
    data_full   = joined3,   
    data_subset = R1_pro,
    output_csv  = out_csv,
    output_rds  = out_rds
  )
}

