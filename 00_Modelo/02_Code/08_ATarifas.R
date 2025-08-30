# ASIGNACION TARIFAS
asignar_tarifa_escenario <- function(
    esc,
    id_escenario,
    seed,
    tarifas = NULL   # vector manual opcional (mismo largo que filas del escenario)
){
  stopifnot(is.data.frame(esc))
  if (!"id_escenario" %in% names(esc)) stop("Falta la columna 'id_escenario' en 'esc'.")
  if (length(id_escenario) != 1L) stop("Solo se permite un 'id_escenario' a la vez.")
  if (missing(seed) || is.null(seed)) stop("Debes proporcionar 'seed' (semilla).")
  
  # Rango fijo (interno)
  MIN_TARIFA <- 1L
  MAX_TARIFA <- 15L
  pool <- seq.int(MIN_TARIFA, MAX_TARIFA)
  
  # 1) Filtrar el grupo
  sub <- dplyr::filter(esc, id_escenario == !!id_escenario)
  n <- nrow(sub)
  if (n == 0L) stop("No hay filas para id_escenario = ", id_escenario, ".")
  
  # 2) Asignar tarifas
  set.seed(seed)
  
  if (is.null(tarifas)) {
    # Aleatorio por fila (preferir sin reemplazo cuando alcance)
    if (n <= length(pool)) {
      vals <- sample(pool, n, replace = FALSE)
    } else {
      vals <- sample(pool, n, replace = TRUE)
      # Evitar el caso degenerado de todas iguales si n>1
      if (n > 1L && length(unique(vals)) == 1L) {
        idx <- sample.int(n, 1L)
        vals[idx] <- sample(setdiff(pool, vals[1L]), 1L)
      }
    }
  } else {
    # Difinir tarifas
    if (length(tarifas) != n)
      stop("El largo de 'tarifas' (", length(tarifas), 
           ") debe igualar las filas del escenario (", n, ").")
    vals <- as.integer(tarifas)
    if (n > 1L && length(unique(vals)) == 1L)
      stop("Para n>1, 'tarifas' no puede ser el mismo valor en todas las filas.")
  }
  
  dplyr::mutate(sub, tarifa = as.integer(vals))
}
