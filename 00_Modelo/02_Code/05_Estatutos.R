# FUNCION 5 
# ESTATUTOS TRIBUTARIOS
# PAQUETES
library(dplyr)
library(readr)
library(janitor)
library(stringr)
library(lubridate)

#excel_sheets("01_Data/Org/Plantilla_EstatutosTributarios.xlsx")
#es1 <- read_excel("01_Data/Org/Plantilla_EstatutosTributarios.xlsx", sheet = "BD_Transcripciones")
#es_gtpe <- es1 %>%
#  filter(municipio == "Guatapé")


estatuto <- function(path_xlsx,
                                       municipio,
                                       sheet = "BD_Transcripciones",
                                       col_municipio = "municipio",
                                       decimal_comma = FALSE,
                                       out_dir = "01_Data/Derived",
                                       file_stub = NULL) {
  

  norm_text <- function(x) { 
    y <- as.character(x)
    y <- str_squish(y)
    y <- iconv(y, to = "ASCII//TRANSLIT")
    tolower(y)
  }
  norm_name <- function(x) {  
    y <- iconv(x, to = "ASCII//TRANSLIT")
    y <- tolower(y)
    gsub("[^a-z0-9]+", "", y)
  }
  
  # 1) Leer
  df <- readxl::read_excel(path_xlsx, sheet = sheet)
  
  # 2) Columna municipio 
  pos_mun <- which(norm_name(names(df)) == norm_name(col_municipio))
  stopifnot(length(pos_mun) > 0)
  col_mun <- names(df)[pos_mun[1]]
  
  # 3) Filtrar 
  out <- df[norm_text(df[[col_mun]]) == norm_text(municipio), , drop = FALSE]
  
  # 4)  DESTINO_ECONOMICO
  pos_dest <- which(norm_name(names(out)) == norm_name("DESTINO_ECONOMICO"))
  if (length(pos_dest)) names(out)[pos_dest[1]] <- "DESTINO_ECONOMICO"
  out <- out %>%
    rename("ZONA" = "sector")
  
  # 5) Reemplazos en DESTINO_ECONOMICO 
  replacements <- c(
    "agricola" = "Agricola",
    "educativo" = "Educativo",
    "institucional" = "Institucional",
    "industrial" = "Industrial",
    "comercial" = "Comercial",
    "lote urbanizable no urbanizado" = "Lote urbanizable no urbanizado",
    "cultural" = "Cultural",
    "forestal" = "Forestal",
    "habitacional" = "Habitacional",
    "salubridad" = "Salubridad",
    "lote no urbanizable" = "Lote no urbanizable",
    "lote urbanizado no construido" = "Lote urbanizado no construido",
    "religioso" = "Religioso",
    "uso publico" = "Uso publico",
    "mineros e hidrocarburos" = "Mineros e hidrocarburos",
    "pecuario" = "Pecuario",
    "recreacional" = "Recreacional",
    "servicios especiales" = "Servicios especiales"
  )
  if ("DESTINO_ECONOMICO" %in% names(out) && length(replacements)) {
    key <- norm_text(out$DESTINO_ECONOMICO)
    rep_norm <- setNames(unname(replacements), names(replacements)) 
    hit <- !is.na(key) & key %in% names(rep_norm)
    out$DESTINO_ECONOMICO[hit] <- rep_norm[key[hit]]
  }
  
  # 
  out <- out %>%
  mutate(sector = recode (ZONA,
                          "rural" = "Rural",
                          "urbano" = "Urbano"))
  
  # 6) Convertir a numérico 
  targets <- c("rl_Avalúo","rh_Avalúo","rl_area","rh_area","estrato","tarifa")
  for (tgt in targets) {
    pos <- which(norm_name(names(out)) == norm_name(tgt))
    if (!length(pos)) next
    col <- names(out)[pos[1]]
    x <- as.character(out[[col]])
    x[x %in% c("", "NA", "Na", "na")] <- NA
    if (decimal_comma) x <- gsub(",", ".", x, fixed = TRUE)
    out[[col]] <- suppressWarnings(as.numeric(x))
  }
  
  # 7) Guardar en 01_Data/Derived 
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  muni_slug <- gsub("[^A-Za-z0-9]+", "_", municipio)
  if (is.null(file_stub)) {
    base <- tools::file_path_sans_ext(basename(path_xlsx))
    file_stub <- paste0(base, "_", muni_slug)
  }
  xlsx_path <- file.path(out_dir, paste0(file_stub, ".xlsx"))
  rda_path  <- file.path(out_dir, paste0(file_stub, ".rda"))
  

  writexl::write_xlsx(out, xlsx_path)
  BD_filtrada <- out
  save(BD_filtrada, file = rda_path)
  return(out)
}
