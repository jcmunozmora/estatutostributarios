
escenarios_reglas <- function(
    r1,
    esc_filtrado,                
    col_avaluo  = "AVALUO",
    col_area    = "AREA_TERRENO",
    tarifa_base = c("permil","percent","proportion")
){
  stopifnot(is.data.frame(r1), is.data.frame(esc_filtrado))
  tarifa_base <- match.arg(tarifa_base)
  
  if (!"tarifa" %in% names(esc_filtrado))
    stop("'esc_filtrado' debe traer una columna 'tarifa'.")
  
  #  Si falta la columna, devuelve NA del tamaño correcto
  make_or_na <- function(df, nm, as_fun = identity) {
    if (nm %in% names(df)) as_fun(df[[nm]]) else rep(NA, nrow(df))
  }
  
  # Alineacion de nombres
  esc_m <- dplyr::mutate(
    esc_filtrado,
    # AVALÚO
    `rl_Avalúo` = dplyr::coalesce(
      suppressWarnings(as.numeric(if ("rl_Avaluo" %in% names(esc_filtrado)) esc_filtrado$rl_Avaluo else NA_real_)),
      suppressWarnings(as.numeric(if ("rl_Avalúo" %in% names(esc_filtrado)) esc_filtrado$`rl_Avalúo` else NA_real_))
    ),
    `rh_Avalúo` = dplyr::coalesce(
      suppressWarnings(as.numeric(if ("rh_Avaluo" %in% names(esc_filtrado)) esc_filtrado$rh_Avaluo else NA_real_)),
      suppressWarnings(as.numeric(if ("rh_Avalúo" %in% names(esc_filtrado)) esc_filtrado$`rh_Avalúo` else NA_real_))
    ),
    # ÁREA (acepta mayúscula/minúscula)
    rl_area = suppressWarnings(as.numeric(if ("rl_Area" %in% names(esc_filtrado)) esc_filtrado$rl_Area else if ("rl_area" %in% names(esc_filtrado)) esc_filtrado$rl_area else NA_real_)),
    rh_area = suppressWarnings(as.numeric(if ("rh_Area" %in% names(esc_filtrado)) esc_filtrado$rh_Area else if ("rh_area" %in% names(esc_filtrado)) esc_filtrado$rh_area else NA_real_)),
    tarifa  = suppressWarnings(as.numeric(tarifa))
  )
  
  # Construir 'reglas' 
  reglas <- tibble::tibble(
    DESTINO_ECONOMICO     = make_or_na(esc_m, "DESTINO_ECONOMICO", as.character),
    subdestino_economico  = make_or_na(esc_m, "subdestino_economico", as.character), # si no existe, queda NA (comodín)
    ZONA                  = make_or_na(esc_m, "ZONA", as.character),
    estrato               = make_or_na(esc_m, "estrato", as.character),
    nro_predios           = make_or_na(esc_m, "nro_predios", as.character),
    `rl_Avalúo`           = esc_m$`rl_Avalúo`,
    `rh_Avalúo`           = esc_m$`rh_Avalúo`,
    rl_area               = esc_m$rl_area,
    rh_area               = esc_m$rh_area,
    tarifa                = esc_m$tarifa
  )
  
  # Validación rápida de tarifas
  if (any(is.na(reglas$tarifa)))
    stop("Hay NA en 'tarifa' dentro de esc_filtrado. Revisa que todas las filas traigan un valor.")
  
  # Aplicar reglas sobre R1 usando tu función original
  asignar_tarifa(
    r1          = r1,
    reglas      = reglas,
    col_avaluo  = col_avaluo,
    col_area    = col_area,
    tarifa_base = tarifa_base
  )
}
