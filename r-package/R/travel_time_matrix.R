#' Calculate travel time matrix between origin destination pairs considering a
#' departure time
#'
#' Fast computation of travel time estimates between one or multiple origin
#' destination pairs. This function considers a departure time set by the user.
#' If you want to calculate travel times considering a time of arrival, have a
#' look at the [arrival_travel_time_matrix()] function.
#'
#' @template r5r_network
#' @template r5r_core
#' @template common_arguments
#' @template time_window_related_args
#' @template draws_per_minute
#' @template fare_structure
#' @template max_fare
#' @template scenarios
#' @template verbose
#' @param percentiles An integer vector (max length of 5). Specifies the
#'   percentile to use when returning travel time estimates within the given
#'   time window. For example, if the 25th travel time percentile between A and
#'   B is 15 minutes, 25% of all trips taken between these points within the
#'   specified time window are shorter than 15 minutes. Defaults to 50,
#'   returning the median travel time. If a vector with length bigger than 1 is
#'   passed, the output contains an additional column for each percentile
#'   specifying the percentile travel time estimate. each estimate. Due to
#'   upstream restrictions, only 5 percentiles can be specified at a time. For
#'   more details, please see R5 documentation at
#'   <https://docs.conveyal.com/analysis/methodology#accounting-for-variability>.
#'
#' @return A `data.table` with travel time estimates (in minutes) between
#'   origin and destination pairs. Pairs whose trips couldn't be completed
#'   within the maximum travel time and/or whose origin is too far from the
#'   street network are not returned in the `data.table`. If `output_dir` is
#'   not `NULL`, the function returns the path specified in that parameter, in
#'   which the `.csv` files containing the results are saved.
#'
#' @template transport_modes_section
#' @template lts_section
#' @template datetime_parsing_section
#' @template raptor_algorithm_section
#'
#' @family routing
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' library(r5r)
#'
#' # build transport network
#' data_path <- system.file("extdata/poa", package = "r5r")
#' r5r_network <- build_network(data_path)
#'
#' # load origin/destination points
#' points <- read.csv(file.path(data_path, "poa_points_of_interest.csv"))
#'
#' departure_datetime <- as.POSIXct(
#'   "13-05-2019 14:00:00",
#'   format = "%d-%m-%Y %H:%M:%S"
#' )
#'
#' ttm <- travel_time_matrix(
#'   r5r_network,
#'   origins = points,
#'   destinations = points,
#'   mode = c("WALK", "TRANSIT"),
#'   departure_datetime = departure_datetime,
#'   max_trip_duration = 60
#' )
#' head(ttm)
#'
#' # using a larger time window
#' ttm <- travel_time_matrix(
#'   r5r_network,
#'   origins = points,
#'   destinations = points,
#'   mode = c("WALK", "TRANSIT"),
#'   departure_datetime = departure_datetime,
#'   time_window = 30,
#'   max_trip_duration = 60
#' )
#' head(ttm)
#'
#' # selecting different percentiles
#' ttm <- travel_time_matrix(
#'   r5r_network,
#'   origins = points,
#'   destinations = points,
#'   mode = c("WALK", "TRANSIT"),
#'   departure_datetime = departure_datetime,
#'   time_window = 30,
#'   percentiles = c(25, 50, 75),
#'   max_trip_duration = 60
#' )
#' head(ttm)
#'
#' # use a fare structure and set a max fare to take monetary constraints into
#' # account
#' fare_structure <- read_fare_structure(
#'   file.path(data_path, "fares/fares_poa.zip")
#' )
#' ttm <- travel_time_matrix(
#'   r5r_network,
#'   origins = points,
#'   destinations = points,
#'   mode = c("WALK", "TRANSIT"),
#'   departure_datetime = departure_datetime,
#'   fare_structure = fare_structure,
#'   max_fare = 5,
#'   max_trip_duration = 60,
#' )
#' head(ttm)
#'
#' stop_r5(r5r_network)
#'
#' @export
travel_time_matrix <- function(r5r_network,
                               r5r_core = deprecated(),
                               origins,
                               destinations,
                               mode = "WALK",
                               mode_egress = "WALK",
                               departure_datetime = Sys.time(),
                               time_window = 10L,
                               percentiles = 50L,
                               max_walk_time = Inf,
                               max_bike_time = Inf,
                               max_car_time = Inf,
                               max_trip_duration = 120L,
                               walk_speed = 3.6,
                               bike_speed = 12,
                               max_rides = 3,
                               max_lts = 2,
                               fare_structure = NULL,
                               max_fare = Inf,
                               new_carspeeds = NULL,
                               carspeed_scale = 1,
                               new_lts = NULL,
                               draws_per_minute = 5L,
                               n_threads = Inf,
                               verbose = FALSE,
                               progress = FALSE,
                               output_dir = NULL,
                               output_db = NULL,
                               scenario_id = 0L,
                               db_queue_capacity = 64L,
                               db_commit_every = 1000L,
                               db_compression_level = 6L,
                               db_wal_autocheckpoint = 0L) {

  # deprecating r5r_core --------------------------------------
  if (lifecycle::is_present(r5r_core)) {

    cli::cli_warn(c(
      "!" = "The `r5r_core` argument is deprecated as of r5r v2.3.0.",
      "i" = "Please use the `r5r_network` argument instead."
    ))

    r5r_network <- r5r_core
  }

  old_options <- options(datatable.optimize = Inf)
  on.exit(options(old_options), add = TRUE)

  old_dt_threads <- data.table::getDTthreads()
  dt_threads <- ifelse(is.infinite(n_threads), 0, n_threads)
  data.table::setDTthreads(dt_threads)
  on.exit(data.table::setDTthreads(old_dt_threads), add = TRUE)

  # check inputs and set r5r options --------------------------------------

  origins <- assign_points_input(origins, "origins")
  destinations <- assign_points_input(destinations, "destinations")
  mode_list <- assign_mode(mode, mode_egress)
  departure <- assign_departure(departure_datetime)

  # check availability of transit services on the selected date
  if (mode_list$transit_mode %like% 'TRANSIT|TRAM|SUBWAY|RAIL|BUS|CABLE_CAR|GONDOLA|FUNICULAR') {
    check_transit_availability_on_date(r5r_network, departure_date = departure$date)
  }

  checkmate::assert_class(r5r_network, "r5r_network")
  r5r_network <- r5r_network@jcore

  # direct-to-DB output orchestration (koti-db-sink) --------------------------
  # Public function runs ONCE; resume is input substitution (origins <- todo),
  # never a recursive travel_time_matrix() call.
  if (!is.null(output_db)) {
    if (!is.null(output_dir)) {
      stop("'output_dir' and 'output_db' are mutually exclusive.", call. = FALSE)
    }
    checkmate::assert_string(output_db)
    checkmate::assert_count(db_queue_capacity, positive = TRUE)
    checkmate::assert_count(db_commit_every, positive = TRUE)
    checkmate::assert_int(db_compression_level, lower = 0, upper = 9)
    checkmate::assert_int(db_wal_autocheckpoint, lower = 0)
    if (max_trip_duration > 65534) {
      stop("output_db supports max_trip_duration <= 65534 minutes (uint16).", call. = FALSE)
    }
    if (anyDuplicated(origins$id) > 0 || anyDuplicated(destinations$id) > 0) {
      stop("output_db requires unique origin and destination ids (DB primary key).",
           call. = FALSE)
    }
    # v1 supports only square matrices: ordered IDs AND coordinates must match.
    # (fingerprint = ordered id + lon + lat; same hash feeds meta origin/dest_hash)
    if (!identical(ttm_fingerprint(origins), ttm_fingerprint(destinations))) {
      stop("output_db v1 supports only square matrices with identical ordered ",
           "origin/destination IDs and coordinates.", call. = FALSE)
    }

    ttm_meta <- list(
      schema_version      = "1",
      payload_version     = "1",
      layout              = "1",
      codec               = "1",
      compression_level   = as.character(db_compression_level),
      unit                = "minutes",
      sentinel            = if (max_trip_duration <= 254) "uint8_255" else "uint16_65535",
      width               = if (max_trip_duration <= 254) "1" else "2",
      mode                = paste(mode, collapse = ";"),
      departure_datetime  = format(departure_datetime, "%Y-%m-%d %H:%M:%S"),
      time_window_size    = as.character(time_window),
      max_trip_duration   = as.character(max_trip_duration),
      percentiles         = paste(percentiles, collapse = ","),
      n_dest              = as.character(nrow(destinations)),
      network_file        = basename(r5r_network$getDataPath()),
      r5r_version         = as.character(utils::packageVersion("r5r")),
      r5_version          = r5r_env$r5_jar_version,
      sqlite_jdbc_version = "3.53.2.1"
    )
    ttm_init_or_validate_db(output_db, destinations, ttm_meta, scenario_id)

    origins <- ttm_resume_origins(output_db, origins, scenario_id)
    if (nrow(origins) == 0L) {
      message("Scenario already complete: ", output_db)
      return(output_db)                       # no linkage rebuild at all
    }
  }

  # in direct modes reverse origin/destination to take advantage of R5's One to Many algorithm
  data_path <- r5r_network$getDataPath()
  res <- reverse_if_direct_mode(origins, destinations, mode_list, data_path)
  origins <- res$origins
  destinations <- res$destinations


  max_walk_time <- assign_max_street_time(
    max_walk_time,
    walk_speed,
    max_trip_duration,
    "walk"
  )
  max_bike_time <- assign_max_street_time(
    max_bike_time,
    bike_speed,
    max_trip_duration,
    "bike"
  )
  max_car_time <- assign_max_street_time(
    max_car_time,
    8, # 8 km/h, R5's default.
    max_trip_duration,
    "car"
  )
  max_trip_duration <- assign_max_trip_duration(
    max_trip_duration,
    mode_list,
    max_walk_time,
    max_bike_time
  )

  set_time_window(r5r_network, time_window)
  set_percentiles(r5r_network, percentiles)
  set_monte_carlo_draws(r5r_network, draws_per_minute, time_window)
  set_speed(r5r_network, walk_speed, "walk")
  set_speed(r5r_network, bike_speed, "bike")
  set_max_rides(r5r_network, max_rides)
  set_max_lts(r5r_network, max_lts)
  set_n_threads(r5r_network, n_threads)
  set_verbose(r5r_network, verbose)
  set_progress(r5r_network, progress)
  set_fare_structure(r5r_network, fare_structure)
  set_max_fare(r5r_network, max_fare)
  set_output_dir(r5r_network, output_dir)
  set_output_db(r5r_network, output_db, scenario_id,
                db_queue_capacity, db_commit_every,
                db_compression_level, db_wal_autocheckpoint)
  set_expanded_travel_times(r5r_network, FALSE)
  set_breakdown(r5r_network, FALSE)
  r5r_network$setSearchType("DEPART_FROM")

  # SCENARIOS -------------------------------------------
  set_new_congestion(r5r_network, new_carspeeds, carspeed_scale)
  set_new_lts(r5r_network, new_lts)


  # call r5r_network method and process result -------------------------------

  travel_times <- r5r_network$travelTimeMatrix(
    origins$id,
    origins$lat,
    origins$lon,
    destinations$id,
    destinations$lat,
    destinations$lon,
    mode_list$direct_modes,
    mode_list$transit_mode,
    mode_list$access_mode,
    mode_list$egress_mode,
    departure$date,
    departure$time,
    max_walk_time,
    max_bike_time,
    max_car_time,
    max_trip_duration
  )

  if (!is.null(output_db)) return(output_db)   # DB mode: Java returned an empty frame

  if (!verbose & progress) cat("Preparing final output...", file = stderr())

  travel_times <- java_to_dt(travel_times)

  # reverse order of origins destinations back
  travel_times <- reverse_back_if_direct_mode(travel_times, origins, destinations, mode_list, data_path)


  if (nrow(travel_times) > 0) {
    # replace travel-times of nonviable trips with NAs.
    # the first column with travel time information is column 3, because
    # columns 1 and 2 contain the ids of OD point.
    # the percentiles parameter indicates how many travel times columns we'll
    # have
    for (j in seq(from = 3, to = (length(percentiles) + 2))) {
      data.table::set(
        travel_times,
        i = which(travel_times[[j]] > max_trip_duration),
        j = j,
        value = NA_integer_
      )
    }
  }

  if (!verbose & progress) cat(" DONE!\n", file = stderr())

  if (!is.null(output_dir)) return(output_dir)
  return(travel_times[])
}
