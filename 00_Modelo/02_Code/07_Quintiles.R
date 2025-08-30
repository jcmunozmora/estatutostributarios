
generar_escenarios_powerset <- function(
    r1,
    tokens = c("ZONA","DESTINO_ECONOMICO","AVALUO","AREA","ESTRATO","NRO_PREDIOS","CONFLICTO"),
    avaluo_var = "AVALUO",
    area_var   = "AREA_TERRENO",
    niveles = list(
      DESTINO_ECONOMICO = NULL,            # si NULL, toma únicos de r1
      ZONA              = NULL,            # si NULL, toma únicos de r1
      ESTRATO           = as.character(1:6),
      NRO_PREDIOS       = c("uno","uno o mas"),
      CONFLICTO         = c(0L,1L),
      ADECUADO          = c(0L,1L),
      SOBREUTILIZACION  = c(0L,1L),
      NOCONFLICTO       = c(0L,1L),
      DEUDOR            = c(0L,1L)
    ),
    probs = c(0,.20,.40,.60,.80,1),        # quintiles
    qtype = 7,
    max_rows_per_group = 1e6,
    max_k = NULL,                          # NULL = power set completo
    output_rda = "01_Data/Derived/escenarios.rda",
    object_name = "escenarios",
    compress = "xz"
){
  stopifnot(is.data.frame(r1))
  suppressPackageStartupMessages({
    library(dplyr); library(tidyr); library(tibble); library(purrr)
  })
  
  # ---- helpers
  uniq_chr <- function(x) sort(unique(stats::na.omit(as.character(x))))
  quant_or_stop <- function(x, nm){
    x <- suppressWarnings(as.numeric(x))
    if (all(is.na(x))) stop("La columna '", nm, "' no tiene datos numéricos válidos.")
    as.numeric(stats::quantile(x, probs = probs, na.rm = TRUE, type = qtype, names = FALSE))
  }
  
  # ---- niveles base
  DESTINO_tbl <- if (is.null(niveles$DESTINO_ECONOMICO)) {
    if (!"DESTINO_ECONOMICO" %in% names(r1)) stop("Falta 'DESTINO_ECONOMICO' en r1.")
    tibble(DESTINO_ECONOMICO = uniq_chr(r1$DESTINO_ECONOMICO))
  } else tibble(DESTINO_ECONOMICO = as.character(niveles$DESTINO_ECONOMICO))
  
  ZONA_tbl <- if (is.null(niveles$ZONA)) {
    if (!"ZONA" %in% names(r1)) stop("Falta 'ZONA' en r1.")
    tibble(ZONA = uniq_chr(r1$ZONA))
  } else tibble(ZONA = as.character(niveles$ZONA))
  
  ESTRATO_tbl          <- tibble(estrato = as.character(niveles$ESTRATO))
  NRO_PREDIOS_tbl      <- tibble(nro_predios = as.character(niveles$NRO_PREDIOS))
  CONFLICTO_tbl        <- tibble(conflicto = as.integer(niveles$CONFLICTO))
  ADECUADO_tbl         <- tibble(adecuado = as.integer(niveles$ADECUADO))
  SOBREUTILIZACION_tbl <- tibble(sobreutilizacion = as.integer(niveles$SOBREUTILIZACION))
  NOCONFLICTO_tbl      <- tibble(noconflicto = as.integer(niveles$NOCONFLICTO))
  DEUDOR_tbl           <- tibble(deudor = as.integer(niveles$DEUDOR))
  
  # ---- quintiles para Avalúo y Área
  if (!avaluo_var %in% names(r1)) stop("No encuentro '", avaluo_var, "' en r1.")
  if (!area_var   %in% names(r1)) stop("No encuentro '", area_var,   "' en r1.")
  qs_av <- quant_or_stop(r1[[avaluo_var]], avaluo_var)
  qs_ar <- quant_or_stop(r1[[area_var]],   area_var)
  
  AVALUO_tbl <- tibble(
    rl_Avaluo = c(qs_av[1], qs_av[2], qs_av[3], qs_av[4], qs_av[5]),
    rh_Avaluo = c(qs_av[2], qs_av[3], qs_av[4], qs_av[5], qs_av[6])
  )
  AREA_tbl <- tibble(
    rl_Area = c(qs_ar[1], qs_ar[2], qs_ar[3], qs_ar[4], qs_ar[5]),
    rh_Area = c(qs_ar[2], qs_ar[3], qs_ar[4], qs_ar[5], qs_ar[6])
  )
  
  # ---- token -> bloque
  pieza <- list(
    ZONA              = ZONA_tbl,
    DESTINO_ECONOMICO = DESTINO_tbl,
    ESTRATO           = ESTRATO_tbl,
    NRO_PREDIOS       = NRO_PREDIOS_tbl,
    CONFLICTO         = CONFLICTO_tbl,
    ADECUADO          = ADECUADO_tbl,
    SOBREUTILIZACION  = SOBREUTILIZACION_tbl,
    NOCONFLICTO       = NOCONFLICTO_tbl,
    DEUDOR            = DEUDOR_tbl,
    AVALUO            = AVALUO_tbl,
    AREA              = AREA_tbl
  )
  
  # ---- columnas y tipos objetivo
  out_cols <- c(
    "id_escenario","id_combo",
    "DESTINO_ECONOMICO","ZONA",
    "rl_Avaluo","rh_Avaluo",
    "rl_Area","rh_Area",
    "estrato","nro_predios",
    "conflicto","adecuado","sobreutilizacion","noconflicto","deudor"
  )
  chr_cols <- c("DESTINO_ECONOMICO","ZONA","estrato","nro_predios")
  num_cols <- c("rl_Avaluo","rh_Avaluo","rl_Area","rh_Area")
  bin_cols <- c("conflicto","adecuado","sobreutilizacion","noconflicto","deudor")
  
  # ---- expandir "CONFLICTO" a sus banderas si viene en tokens
  expand_token <- function(tok){
    if (tok == "CONFLICTO")
      return(c("CONFLICTO","ADECUADO","SOBREUTILIZACION","NOCONFLICTO","DEUDOR"))
    tok
  }
  toks <- unlist(lapply(tokens, expand_token), use.names = FALSE)
  
  # ---- power set 1..K
  K <- if (is.null(max_k)) length(toks) else min(max_k, length(toks))
  subsets <- unlist(lapply(1:K, function(k) combn(toks, k, simplify = FALSE)), recursive = FALSE)
  grupos  <- lapply(subsets, identity)
  names(grupos) <- as.character(seq_along(grupos))
  
  # ---- construir cada grupo directamente (sin función extra)
  hacer_grupo <- function(tokens, gid){
    parts <- pieza[tokens]
    if (any(vapply(parts, is.null, logical(1)))) {
      faltan <- tokens[vapply(parts, is.null, logical(1))]
      stop("Grupo ", gid, ": faltan piezas -> ", paste(faltan, collapse=", "))
    }
    g <- purrr::reduce(parts, tidyr::crossing)
    
    if (nrow(g) > max_rows_per_group)
      stop("Grupo ", gid, " generaría ", format(nrow(g), big.mark=","), " filas (> max_rows_per_group).")
    
    # Tipos
    pres_chr <- intersect(chr_cols, names(g))
    pres_num <- intersect(num_cols, names(g))
    pres_bin <- intersect(bin_cols, names(g))
    if (length(pres_chr)) g <- dplyr::mutate(g, dplyr::across(dplyr::all_of(pres_chr), as.character))
    if (length(pres_num)) g <- dplyr::mutate(g, dplyr::across(dplyr::all_of(pres_num), as.numeric))
    if (length(pres_bin)) g <- dplyr::mutate(g, dplyr::across(dplyr::all_of(pres_bin), as.integer))
    
    # completar ausentes con NA 
    miss_chr <- setdiff(chr_cols, names(g))
    miss_num <- setdiff(num_cols, names(g))
    miss_bin <- setdiff(bin_cols, names(g))
    if (length(miss_chr)) for (m in miss_chr) g[[m]] <- NA_character_
    if (length(miss_num)) for (m in miss_num) g[[m]] <- NA_real_
    if (length(miss_bin)) for (m in miss_bin) g[[m]] <- NA_integer_
    
    g %>%
      dplyr::mutate(
        id_escenario = as.integer(gid),
        id_combo     = dplyr::row_number(),
        .before = 1
      ) %>%
      dplyr::select(dplyr::all_of(out_cols))
  }
  
  esc <- purrr::imap_dfr(grupos, hacer_grupo)
  
  # ---- guardar .rda
  dir.create(dirname(output_rda), recursive = TRUE, showWarnings = FALSE)
  tmpenv <- new.env(parent = emptyenv())
  assign(object_name, esc, envir = tmpenv)
  save(list = object_name, file = output_rda, envir = tmpenv, compress = compress)
  
  esc
}

