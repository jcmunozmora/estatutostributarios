library(readxl)
library(dplyr)
library(stringr)


conflictos <- function(path_xlsx,
                       out_dir = "01_Data/Derived") {
  
  # --- normalizadores para detectar hojas con tildes/mayúsculas ---
  norm <- function(x) {
    x <- iconv(x, to = "ASCII//TRANSLIT")
    tolower(gsub("\\s+", " ", trimws(x)))
  }
  norm_name <- function(x) {
    y <- gsub("\\.+\\d+$", "", x)   # quita sufijos ...1, ...2 de readxl
    y <- norm(y)
    gsub("[^a-z0-9]+", "", y)
  }
  
  # 1) detectar hojas *_RURAL y *_URBANO
  sheets  <- readxl::excel_sheets(path_xlsx)
  nsheets <- norm_name(sheets)
  sh_rural  <- sheets[which(grepl("rural$",  nsheets))[1]]
  sh_urbano <- sheets[which(grepl("urbano$", nsheets))[1]]
  if (is.na(sh_rural) || is.na(sh_urbano)) {
    stop("Debe haber dos hojas: *_RURAL y *_URBANO. Hojas: ", paste(sheets, collapse = ", "))
  }
  
  # 2) leer
  rural  <- readxl::read_excel(path_xlsx, sheet = sh_rural)
  urbano <- readxl::read_excel(path_xlsx, sheet = sh_urbano)
  
  # 3) renombrar EN CADA HOJA a PK_PREDIOS / TERRENO_CO 
  fix_names <- function(df) {
    n <- names(df); m <- norm_name(n)
    if (!"PK_PREDIOS" %in% n && any(m == "localid"))     n[which(m == "localid")[1]]     <- "PK_PREDIOS"
    if (!"TERRENO_CO" %in% n && any(m == "terrenoco"))   n[which(m == "terrenoco")[1]]   <- "TERRENO_CO"
    if (!"Conflicto" %in% n && any(m == "conflicto"))    n[which(m == "conflicto")[1]]   <- "Conflicto"
    names(df) <- n
    df
  }
  rural  <- fix_names(rural)
  urbano <- fix_names(urbano)
  
  # 4) añadir zona
  rural$zona  <- "Rural"
  urbano$zona <- "Urbano"
  
  # 5) Append
  out <- dplyr::bind_rows(rural, urbano)
  
  # 6) Filtrar
  pct_cols  <- grep("^\\s*%", names(out), value = TRUE)
  base_cols <- c("TERRENO_CO", "PK_PREDIOS", "Conflicto", "zona")
  out <- dplyr::select(out, dplyr::any_of(c(base_cols, pct_cols)))
  
  # 7) convertir columnas '%' a numérico asumiendo punto decimal (.)
  if (length(pct_cols)) {
    for (cn in pct_cols) {
      x <- as.character(out[[cn]])
      x <- str_squish(x)
      x[x %in% c("", "NA", "Na", "na")] <- NA
      x <- gsub("%", "", x, fixed = TRUE)  
      x <- gsub(",", "", x)                
      # dejamos '.' como decimal
      x_num <- suppressWarnings(as.numeric(x))
      mx <- suppressWarnings(max(x_num, na.rm = TRUE))
      if (is.finite(mx) && mx > 1.0001 && mx <= 100.0001) x_num <- x_num / 100
      out[[cn]] <- x_num
    }
  }
  
  # 8) deduplicar por PK_PREDIOS (prioriza más info en % y, empate, 'Urbano')
  if ("PK_PREDIOS" %in% names(out)) {
    n_before <- nrow(out)
    out <- out %>%
      mutate(
        PK_PREDIOS  = str_squish(as.character(PK_PREDIOS)),
        .info_score = if (length(pct_cols)) rowSums(!is.na(across(all_of(pct_cols)))) else 0L,
        .zona_rank  = case_when(zona == "Urbano" ~ 1L,
                                zona == "Rural"  ~ 2L,
                                TRUE             ~ 3L)
      ) %>%
      arrange(PK_PREDIOS, desc(.info_score), .zona_rank) %>%
      distinct(PK_PREDIOS, .keep_all = TRUE) %>%
      select(-.info_score, -.zona_rank)
    message("Duplicados por PK_PREDIOS eliminados: ", n_before - nrow(out))
  } else {
    warning("No existe PK_PREDIOS en la salida; no puedo deduplicar.")
  }
  
  # 9) exportar a XLSX y RDA 
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  code_stub <- basename(dirname(path_xlsx))      # ej: "05321"
  file_stub <- paste0(code_stub, "_conflictos")
  writexl::write_xlsx(out, file.path(out_dir, paste0(file_stub, ".xlsx")))
  conflictos <- out
  save(conflictos, file = file.path(out_dir, paste0(file_stub, ".rda")))
  
  return(out)
}
