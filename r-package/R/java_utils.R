#' Java object to data.table
#'
#' @description Converts a Java object returned by r5r_network to an R `data.table`
#'
#' @param obj A Java Object reference
#'
#' @return An R data.table
#' @family java support functions
#'
#' @keywords internal
java_to_dt <- function(obj) {

  # check input
  if(class(obj)[1] != "jobjRef"){
    stop("Input must be an object of class 'jobjRef'")}

  # get column names from Java table
  columns <- obj$getColumnNames()

  # get the contents of each column in a vector, and return them in a list
  dt <- lapply(columns, function(column_name) {
    # check column data type, so we can call the appropriate Java function
    column_type <- obj$getColumnType(column_name)

    if (column_type == "String") { v <- obj$getStringColumn(column_name) }
    if (column_type == "Integer") { v <- obj$getIntegerColumn(column_name) }
    if (column_type == "Long") { v <- obj$getLongColumn(column_name) }
    if (column_type == "Double") { v <- obj$getDoubleColumn(column_name) }
    if (column_type == "Boolean") { v <- obj$getBooleanColumn(column_name) }
    return(v)
  })

  # convert list of vectors to a data.table, and rename columns accordingly
  data.table::setDT(dt)
  data.table::setnames(dt, new = columns)
}

#' data.table to speedMap
#'
#' @description Converts a `data.frame` with road OSM id's and respective speeds
#'              to a Java Map<Long, Float> for use by r5r_network.
#'
#' @param dt data.frame/data.table. Table specifying the
#'        speed modifications. The table must contain columns \code{osm_id} and
#'        \code{max_speed}.
#' @return A speedMap (Java HashMap<Long, Float>)
#' @family java support functions
#' @keywords internal
dt_to_speed_map <- function(dt) {
  if (is.null(dt)){
    return (rJava::.jnew("java/util/HashMap"))
  }

  checkmate::assert_names(names(dt), must.include = c("osm_id", "max_speed", "speed_type"))
  checkmate::assert_numeric(dt$osm_id, any.missing = FALSE, all.missing = FALSE)
  checkmate::assert_numeric(dt$max_speed, any.missing = FALSE, all.missing = FALSE)
  checkmate::assert_true(length(unique(dt$speed_type)) == 1 && dt$speed_type[1] %in% c("km/h", "scale"))

  # Create new HashMap<long, float>
  map_builder <- rJava::.jnew("org.ipea.r5r.Utils.RMapBuilder")
  speed_map <- map_builder$buildSpeedMap(paste(format(dt$osm_id, scientific = FALSE, trim = TRUE), collapse = ","),
                                        paste(format(dt$max_speed, scientific = FALSE, trim = TRUE), collapse = ","))

  return(speed_map)
}

#' Convert a carspeeds data.frame to forward/backward Java speed maps
#'
#' @description Direction-aware version. If the data.frame has a `direction`
#'   column (0 = forward/up, 1 = backward/down), rows are split into two maps.
#'   If absent, the same speeds are used for both directions.
#'
#' @param dt data.frame with columns osm_id, max_speed, speed_type, and
#'   optionally direction (0/1).
#' @return A list with elements `fwd` and `bwd`, each a Java HashMap<Long,Float>.
#' @family java support functions
#' @keywords internal
dt_to_speed_maps_dir <- function(dt) {
  if (is.null(dt)) {
    empty <- rJava::.jnew("java/util/HashMap")
    return(list(fwd = empty, bwd = empty))
  }
  if (!"direction" %in% names(dt)) {
    # undirected: same map for both directions
    m <- dt_to_speed_map(dt)
    return(list(fwd = m, bwd = m))
  }
  checkmate::assert_true(all(dt$direction %in% c(0, 1)))
  dt_fwd <- dt[dt$direction == 0, , drop = FALSE]
  dt_bwd <- dt[dt$direction == 1, , drop = FALSE]
  # each split must still satisfy dt_to_speed_map's column checks
  fwd <- if (nrow(dt_fwd)) dt_to_speed_map(dt_fwd) else rJava::.jnew("java/util/HashMap")
  bwd <- if (nrow(dt_bwd)) dt_to_speed_map(dt_bwd) else rJava::.jnew("java/util/HashMap")
  return(list(fwd = fwd, bwd = bwd))
}


#' Determine the Java version installed locally
#'
#' @return The number of the Java version
#' @family java support functions
#' @keywords internal
get_java_version <- function(){
  ver <- rJava::.jcall("java.lang.System", "S", "getProperty", "java.version")
  ver <- as.numeric(gsub("\\..*", "", ver))
  return(ver)
}


#' data.table to ltsMap
#'
#' @description Converts a `data.frame` with road OSM id's and respective LTS
#'              levels a Java Map<Long, Integer> for use by r5r_network.
#'
#' @param dt data.frame/data.table. Table specifying the
#'        LTS levels. The table must contain columns \code{osm_id} and
#'        \code{lts}.
#' @return A speedMap (Java HashMap<Long, Integer>)
#' @family java support functions
#' @keywords internal
dt_to_lts_map <- function(dt) {
  checkmate::assert_names(names(dt), must.include = c("osm_id", "lts"))
  checkmate::assert_numeric(dt$osm_id, any.missing = FALSE, all.missing = FALSE)
  checkmate::assert_integer(dt$lts, any.missing = FALSE, all.missing = FALSE)

  # Create new HashMap<Long, Integer>
  map_builder <- rJava::.jnew("org.ipea.r5r.Utils.RMapBuilder")
  lts_map <- map_builder$buildLtsMap(paste(format(dt$osm_id, scientific = FALSE, trim = TRUE), collapse = ","),
                                         paste(as.character(dt$lts), collapse = ","))

  return(lts_map)
}
