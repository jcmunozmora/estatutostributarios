library(dplyr)
library(stringr)
library(tibble)

asignar_tarifa <- function(r1, reglas,
                           col_avaluo  = "AVALUO",
                           col_area    = "AREA_TERRENO",
                           tarifa_base = c("permil","percent","proportion")) {
  tarifa_base <- match.arg(tarifa_base)  # "permil" (default), "percent", "proportion"
  
  # Helpers
  norm_txt <- function(x){
    x <- as.character(x)
    x <- iconv(x, to = "ASCII//TRANSLIT")
    tolower(str_squish(x))
  }
  to_num <- function(x){
    x <- as.character(x)
    x <- gsub("[%$,]", "", x)
    suppressWarnings(as.numeric(x))
  }
  
  # ----- preparar r1 
  r1w <- as_tibble(r1) %>%
    mutate(
      .row_id    = row_number(),
      .DEST_N    = if ("DESTINO_ECONOMICO" %in% names(.)) norm_txt(DESTINO_ECONOMICO) else NA_character_,
      .SUBDEST_N = if ("subdestino_economico" %in% names(.)) norm_txt(subdestino_economico) else NA_character_,
      .ZONA_N    = if ("ZONA" %in% names(.)) norm_txt(ZONA) else NA_character_,
      .ESTRATO_N = if ("estrato" %in% names(.)) norm_txt(estrato) else NA_character_,
      .NPREDIOS  = dplyr::coalesce(
        suppressWarnings(as.numeric(nro_predios)),
        suppressWarnings(as.numeric(NUM_PREDIOS))
      ),
      .AVALUO    = if (col_avaluo %in% names(.)) to_num(.data[[col_avaluo]]) else NA_real_,
      .AREA      = if (col_area   %in% names(.)) to_num(.data[[col_area]])   else NA_real_
    )
  
  # ----- preparar reglas
  rg <- as_tibble(reglas) %>%
    mutate(
      .rule_id   = row_number(),
      .DEST_N    = norm_txt(DESTINO_ECONOMICO),
      .SUBDEST_N = norm_txt(subdestino_economico),
      .ZONA_N    = if ("ZONA" %in% names(.)) norm_txt(ZONA) else NA_character_,
      .ESTRATO_N = norm_txt(estrato),
      .NPREDIOS  = suppressWarnings(as.numeric(nro_predios)),
      RL_AVAL    = to_num(`rl_Avalúo`),
      RH_AVAL    = to_num(`rh_Avalúo`),
      RL_AREA    = to_num(`rl_area`),
      RH_AREA    = to_num(`rh_area`),
      tarifa     = suppressWarnings(as.numeric(tarifa))
    ) %>%
    mutate(
      # qué condiciones exige cada regla (NA )
      use_dest    = !is.na(DESTINO_ECONOMICO) & DESTINO_ECONOMICO != "",
      use_subdest = !is.na(subdestino_economico) & subdestino_economico != "",
      use_zona    = "ZONA" %in% names(reglas) & !is.na(ZONA) & ZONA != "",
      use_estrato = !is.na(estrato) & estrato != "",
      use_np      = !is.na(.NPREDIOS),
      use_aval    = !is.na(RL_AVAL) | !is.na(RH_AVAL),
      use_area    = !is.na(RL_AREA) | !is.na(RH_AREA),
      # rangos abiertos
      RL_AVAL     = ifelse(is.na(RL_AVAL), -Inf, RL_AVAL),
      RH_AVAL     = ifelse(is.na(RH_AVAL),  Inf, RH_AVAL),
      RL_AREA     = ifelse(is.na(RL_AREA), -Inf, RL_AREA),
      RH_AREA     = ifelse(is.na(RH_AREA),  Inf, RH_AREA),
      # especificidad (más alto = más específica)
      spec_score  = (use_dest + use_subdest + use_zona + use_estrato + use_np + use_aval + use_area),
      width_aval  = ifelse(use_aval, pmax(0, RH_AVAL - RL_AVAL), Inf),
      width_area  = ifelse(use_area, pmax(0, RH_AREA - RL_AREA), Inf)
    ) %>%
    arrange(desc(spec_score), width_aval, width_area, .rule_id)
  
  # ----- aplicar reglas 
  tarifa_out <- rep(NA_real_, nrow(r1w))
  regla_out  <- rep(NA_integer_, nrow(r1w))
  
  for (i in seq_len(nrow(rg))) {
    rr <- rg[i,]
    mask <- is.na(tarifa_out)   
    
    if (rr$use_dest) {
      cond <- (!is.na(r1w$.DEST_N)) & (r1w$.DEST_N == rr$.DEST_N)
      mask <- mask & cond
    }
    if (rr$use_subdest) {
      has_sub <- !is.na(r1w$.SUBDEST_N) & r1w$.SUBDEST_N != ""
      cond <- ifelse(has_sub, r1w$.SUBDEST_N == rr$.SUBDEST_N, TRUE)
      mask <- mask & cond
    }
    if (rr$use_zona) {
      cond <- (!is.na(r1w$.ZONA_N)) & (r1w$.ZONA_N == rr$.ZONA_N)
      mask <- mask & cond
    }
    if (rr$use_estrato) {
      cond <- (!is.na(r1w$.ESTRATO_N)) & (r1w$.ESTRATO_N == rr$.ESTRATO_N)
      mask <- mask & cond
    }
    if (rr$use_np) {
      cond <- (!is.na(r1w$.NPREDIOS)) & (r1w$.NPREDIOS == rr$.NPREDIOS)
      mask <- mask & cond
    }
    if (rr$use_aval) {
      cond <- (!is.na(r1w$.AVALUO)) & (r1w$.AVALUO >= rr$RL_AVAL) & (r1w$.AVALUO <= rr$RH_AVAL)
      mask <- mask & cond
    }
    if (rr$use_area) {
      cond <- (!is.na(r1w$.AREA)) & (r1w$.AREA >= rr$RL_AREA) & (r1w$.AREA <= rr$RH_AREA)
      mask <- mask & cond
    }
    
    if (any(mask, na.rm = TRUE)) {
      idx <- which(mask %in% TRUE)
      tarifa_out[idx] <- rr$tarifa
      regla_out[idx]  <- rr$.rule_id
    }
  }
  
  # ----- calcular avaluo_cal según unidad de tarifa
  divisor <- switch(tarifa_base,
                    permil     = 1000,
                    percent    = 100,
                    proportion = 1)
  
  out <- r1w %>%
    mutate(
      tarifa         = tarifa_out,
      id_regla_match = regla_out,
      avaluo_cal     = .AVALUO * tarifa / divisor
    ) %>%
    select(-starts_with("."))
  
  # total como atributo (no cambia el tipo de retorno)
  attr(out, "total_avaluo_cal") <- sum(out$avaluo_cal, na.rm = TRUE)
  
  out
}

