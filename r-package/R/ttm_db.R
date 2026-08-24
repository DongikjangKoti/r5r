# Direct-to-SQLite TTM output: R-side schema owner and orchestration helpers.
# Design document: R5_TTM_DB_구축전략.md (v6). The R side OWNS the schema;
# the Java sink (TtmSink.java) only validates it and fails fast.

TTM_SCHEMA_VERSION  <- "1"
TTM_PAYLOAD_VERSION <- 1L

# immutable fingerprint keys: resume is refused unless ALL of these exist and match.
# Review #1 (2026-08-24): the fingerprint must identify the FULL routing semantics -
# in this project new_carspeeds changes hourly, and a resume with the wrong hour's
# speeds would silently mix scenarios. Large objects enter as xxhash64 hashes;
# effective (post-assign) scalars enter as values so a mismatch names the culprit key.
TTM_ROUTING_KEYS <- c(
  "mode", "mode_egress", "departure_datetime", "time_window_size",
  "max_trip_duration", "max_walk_time", "max_bike_time", "max_car_time",
  "walk_speed", "bike_speed", "max_rides", "max_lts", "max_fare", "fare_hash",
  "draws_per_minute", "carspeed_scale", "carspeeds_hash", "new_lts_hash",
  "network_file", "network_size", "network_mtime",
  "r5r_version", "r5_version", "sqlite_jdbc_version"
)

TTM_FP_KEYS <- c(
  "schema_version", "payload_version", "layout", "codec", "compression_level",
  "unit", "sentinel", "width", "scenario_id", "percentiles", "n_dest",
  "origin_hash", "dest_hash",
  TTM_ROUTING_KEYS
)

# expTTM: no percentiles/unit/sentinel/width; breakdown flag matters instead
EXPTTM_FP_KEYS <- c(
  "schema_version", "payload_version", "layout", "codec", "compression_level",
  "scenario_id", "breakdown", "n_dest",
  "origin_hash", "dest_hash",
  TTM_ROUTING_KEYS
)

EXPTTM_REQUIRED_TABLES <- c("meta", "dest_index", "origin_index", "expttm_chunk", "done")

#' xxhash64 of an arbitrary R object (NULL-safe, deterministic). Used to bind
#' large routing inputs (new_carspeeds, new_lts, fare_structure) into the fingerprint.
#' @keywords internal
ttm_object_hash <- function(x) digest::digest(x, algo = "xxhash64")

#' Ordered fingerprint of a points table (id + lon + lat + row order).
#' Coordinates are included on purpose: identical IDs with different
#' coordinates are NOT the same routing targets.
#' @keywords internal
ttm_fingerprint <- function(points) {
  digest::digest(
    list(as.character(points$id), points$lon, points$lat),
    algo = "xxhash64"
  )
}

#' Current-run meta completeness. MUST run before the new/existing-DB branch:
#' otherwise a new DB is created with incomplete meta and the error only
#' surfaces on the next resume.
#' @keywords internal
ttm_validate_current_meta <- function(meta_list, fp_keys = TTM_FP_KEYS) {
  missing_run <- setdiff(fp_keys, names(meta_list))
  if (length(missing_run) > 0L) {
    stop(
      "required fingerprint keys missing from current run: ",
      paste(missing_run, collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' DB-side fingerprint completeness and equality (existing DB only).
#' @keywords internal
ttm_validate_db_fingerprint <- function(con, meta_list, fp_keys = TTM_FP_KEYS) {
  old <- DBI::dbGetQuery(con, "SELECT k, v FROM meta")
  for (k in fp_keys) {
    ov <- old$v[old$k == k]
    if (length(ov) != 1L) {
      stop(sprintf(
        "required fingerprint key '%s' missing/duplicated in DB - resume refused", k
      ), call. = FALSE)
    }
    nv <- as.character(meta_list[[k]])
    if (!identical(ov, nv)) {
      stop(sprintf(
        "fingerprint mismatch on '%s': DB='%s' vs run='%s' - resume refused", k, ov, nv
      ), call. = FALSE)
    }
  }
  invisible(TRUE)
}

TTM_REQUIRED_TABLES <- c("meta", "dest_index", "origin_index", "ttm", "done")

#' Explicit DDL. dbWriteTable() auto-creation would silently drop the
#' PK/UNIQUE/NOT NULL constraints that the design depends on.
#' Keep in sync with TtmSink.EXPECTED_SCHEMA_VERSION.
#' @keywords internal
ttm_create_schema <- function(con) {
  DBI::dbExecute(con, "CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
  DBI::dbExecute(con, "CREATE TABLE dest_index (
                         idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)")
  DBI::dbExecute(con, "CREATE TABLE origin_index (
                         idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)")
  DBI::dbExecute(con, "CREATE TABLE ttm (
                         scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL,
                         run_idx INTEGER, n_pct INTEGER NOT NULL, n_dest INTEGER NOT NULL,
                         width INTEGER NOT NULL, codec INTEGER NOT NULL,
                         payload_version INTEGER NOT NULL, layout INTEGER NOT NULL,
                         n_reached INTEGER NOT NULL, payload BLOB NOT NULL,
                         PRIMARY KEY (scenario_id, origin_id)) WITHOUT ROWID")
  DBI::dbExecute(con, "CREATE TABLE done (
                         scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL,
                         elapsed_ms INTEGER, ts INTEGER,
                         PRIMARY KEY (scenario_id, origin_id)) WITHOUT ROWID")
}

#' Initialize a new TTM DB or validate an existing one for resume.
#'
#' State machine:
#'   A. no DB file                 -> create schema + index tables + meta (one transaction)
#'   B. DB exists, ttm empty       -> fingerprint must match; never re-initialize
#'   C. DB exists, ttm has rows    -> fingerprint must match; resume only
#'   D. incomplete schema          -> stop
#' @keywords internal
db_init_or_validate <- function(db_path, destinations_full, meta_list, scenario_id,
                                fp_keys, create_fn, required_tables) {
  meta_list$dest_hash   <- ttm_fingerprint(destinations_full)
  meta_list$origin_hash <- meta_list$dest_hash   # v1: square only, same set by definition
  meta_list$scenario_id <- as.character(scenario_id)

  ttm_validate_current_meta(meta_list, fp_keys)  # BEFORE the is_new branch

  is_new <- !file.exists(db_path) || file.size(db_path) == 0  # 0-byte file = empty DB
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (is_new) {                                  # state A
    DBI::dbWithTransaction(con, {
      create_fn(con)
      DBI::dbAppendTable(con, "dest_index", data.frame(
        idx = seq_len(nrow(destinations_full)) - 1L,
        grid_id = as.character(destinations_full$id)))
      DBI::dbAppendTable(con, "origin_index", data.frame(
        idx = seq_len(nrow(destinations_full)) - 1L,
        grid_id = as.character(destinations_full$id)))
      DBI::dbAppendTable(con, "meta", data.frame(
        k = names(meta_list), v = as.character(unlist(meta_list))))
    })
    return(invisible("initialized"))
  }

  tabs <- DBI::dbListTables(con)                 # state D check
  if (!all(required_tables %in% tabs)) {
    stop("incomplete schema in existing DB: ",
         paste(setdiff(required_tables, tabs), collapse = ", "),
         call. = FALSE)
  }

  ttm_validate_db_fingerprint(con, meta_list, fp_keys)  # states B/C; never re-initialize
  invisible("validated")
}

ttm_init_or_validate_db <- function(db_path, destinations_full, meta_list, scenario_id) {
  db_init_or_validate(db_path, destinations_full, meta_list, scenario_id,
                      TTM_FP_KEYS, ttm_create_schema, TTM_REQUIRED_TABLES)
}

expttm_init_or_validate_db <- function(db_path, destinations_full, meta_list, scenario_id) {
  db_init_or_validate(db_path, destinations_full, meta_list, scenario_id,
                      EXPTTM_FP_KEYS, expttm_create_schema, EXPTTM_REQUIRED_TABLES)
}

#' expTTM schema. chunk_id kept for future chunking; v1 always writes chunk 0.
#' Keep in sync with ExpTtmSink.EXPECTED_SCHEMA_VERSION.
#' @keywords internal
expttm_create_schema <- function(con) {
  DBI::dbExecute(con, "CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
  DBI::dbExecute(con, "CREATE TABLE dest_index (
                         idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)")
  DBI::dbExecute(con, "CREATE TABLE origin_index (
                         idx INTEGER PRIMARY KEY, grid_id TEXT NOT NULL UNIQUE)")
  DBI::dbExecute(con, "CREATE TABLE expttm_chunk (
                         scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL,
                         chunk_id INTEGER NOT NULL, n_records INTEGER NOT NULL,
                         codec INTEGER NOT NULL, payload_version INTEGER NOT NULL,
                         layout INTEGER NOT NULL, has_breakdown INTEGER NOT NULL,
                         payload BLOB NOT NULL,
                         PRIMARY KEY (scenario_id, origin_id, chunk_id)) WITHOUT ROWID")
  DBI::dbExecute(con, "CREATE TABLE done (
                         scenario_id INTEGER NOT NULL, origin_id TEXT NOT NULL,
                         elapsed_ms INTEGER, ts INTEGER,
                         PRIMARY KEY (scenario_id, origin_id)) WITHOUT ROWID")
}

#' Origins not yet completed for this scenario.
#' @keywords internal
ttm_resume_origins <- function(db_path, origins_full, scenario_id) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  if (!DBI::dbExistsTable(con, "done")) return(origins_full)
  done_ids <- DBI::dbGetQuery(
    con, "SELECT origin_id FROM done WHERE scenario_id = ?",
    params = list(scenario_id))$origin_id
  origins_full[!(as.character(origins_full$id) %in% done_ids), ]
}

#' Decode one origin's payload into a [percentile x destination] matrix.
#' Destination mapping must come from the DB's own dest_index (source of truth).
#' @export
ttm_read_origin <- function(con, scenario_id, origin_id) {
  r <- DBI::dbGetQuery(con,
    "SELECT n_pct, n_dest, width, codec, payload_version, layout, payload
       FROM ttm WHERE scenario_id = ? AND origin_id = ?",
    params = list(scenario_id, origin_id))
  if (nrow(r) == 0) return(NULL)

  stopifnot(r$codec == 1L, r$payload_version == TTM_PAYLOAD_VERSION, r$layout == 1L)

  width <- r$width[1]; n_dest <- r$n_dest[1]; n_pct <- r$n_pct[1]
  sentinel <- if (width == 1L) 255L else 65535L

  raw_v <- memDecompress(r$payload[[1]], type = "gzip")
  expected <- n_dest * n_pct * width
  if (length(raw_v) != expected) {
    stop(sprintf("corrupted/incompatible payload for origin '%s': %d bytes, expected %d",
                 origin_id, length(raw_v), expected), call. = FALSE)
  }

  v <- readBin(raw_v, "integer", n = n_dest * n_pct, size = width,
               signed = FALSE, endian = "little")
  v[v == sentinel] <- NA_integer_
  matrix(v, nrow = n_pct, byrow = TRUE)
}

#' Decode one origin's expTTM payload into a data.frame in LEGACY column order.
#' Times come back as tenths/10, which is bit-identical to the legacy doubles
#' because PathBreakdown getters already round to one decimal place.
#' @export
expttm_read_origin <- function(con, scenario_id, origin_id, from_id = origin_id,
                               apply_cutoff_na = TRUE) {
  r <- DBI::dbGetQuery(con,
    "SELECT chunk_id, n_records, codec, payload_version, layout, has_breakdown, payload
       FROM expttm_chunk WHERE scenario_id = ? AND origin_id = ? ORDER BY chunk_id",
    params = list(scenario_id, origin_id))
  if (nrow(r) == 0) return(NULL)
  stopifnot(all(r$codec == 1L), all(r$payload_version == 1L), all(r$layout == 1L))

  didx <- DBI::dbGetQuery(con, "SELECT idx, grid_id FROM dest_index ORDER BY idx")
  out <- vector("list", nrow(r))

  for (ci in seq_len(nrow(r))) {
    raw_v <- memDecompress(r$payload[[ci]], type = "gzip")
    pos <- 1L
    take <- function(nbytes) {
      v <- raw_v[pos:(pos + nbytes - 1L)]; pos <<- pos + nbytes; v
    }
    u32 <- function(n) readBin(take(4L * n), "integer", n = n, size = 4L, endian = "little")
    u16 <- function(n) readBin(take(2L * n), "integer", n = n, size = 2L,
                               signed = FALSE, endian = "little")
    u8  <- function(n) readBin(take(1L * n), "integer", n = n, size = 1L,
                               signed = FALSE, endian = "little")

    n      <- u32(1L)
    n_dict <- u32(1L)
    has_bd <- u8(1L) == 1L
    stopifnot(n == r$n_records[ci], has_bd == (r$has_breakdown[ci] == 1L))

    to_idx    <- u32(n)                       # signed read is fine (< 2^31 dests)
    dep_sec   <- u32(n)                       # -1 == SENT_U32 (empty departure)
    draw      <- u8(n)
    total_t   <- u16(n)
    route_cd  <- u16(n)
    if (has_bd) {
      access_t <- u16(n); wait_t <- u16(n); ride_t <- u16(n)
      transfer_t <- u16(n); egress_t <- u16(n); n_rides <- u8(n)
    }
    dict <- character(n_dict)
    for (k in seq_len(n_dict)) {
      len <- u16(1L)
      dict[k] <- if (len > 0) rawToChar(take(len)) else ""
    }
    Encoding(dict) <- "UTF-8"
    stopifnot(pos == length(raw_v) + 1L)      # full consumption = layout intact

    dep_chr <- ifelse(dep_sec < 0, "", sprintf("%02d:%02d:%02d",
                 dep_sec %/% 3600L, (dep_sec %% 3600L) %/% 60L, dep_sec %% 60L))
    total   <- ifelse(total_t == 65535L, 2147483647, total_t / 10)  # legacy MAX_VALUE

    df <- data.frame(
      from_id        = from_id,
      to_id          = didx$grid_id[to_idx + 1L],
      departure_time = dep_chr,
      draw_number    = draw,
      stringsAsFactors = FALSE
    )
    if (has_bd) {
      df$access_time   <- ifelse(access_t   == 65535L, 2147483647, access_t / 10)
      df$wait_time     <- ifelse(wait_t     == 65535L, 2147483647, wait_t / 10)
      df$ride_time     <- ifelse(ride_t     == 65535L, 2147483647, ride_t / 10)
      df$transfer_time <- ifelse(transfer_t == 65535L, 2147483647, transfer_t / 10)
      df$egress_time   <- ifelse(egress_t   == 65535L, 2147483647, egress_t / 10)
      df$routes        <- dict[route_cd + 1L]
      df$n_rides       <- n_rides
    } else {
      df$routes        <- dict[route_cd + 1L]
    }
    df$total_time <- total
    out[[ci]] <- df
  }
  df <- do.call(rbind, out)

  # Legacy semantics (expanded_travel_time_matrix.R post-processing): rows whose
  # total_time exceeds max_trip_duration are kept but their values are NA'd.
  if (apply_cutoff_na && nrow(df) > 0) {
    cutoff <- as.numeric(DBI::dbGetQuery(con,
      "SELECT v FROM meta WHERE k = 'max_trip_duration'")$v)
    bad <- df$total_time > cutoff
    if (any(bad)) {
      df$routes[bad] <- NA_character_
      df$total_time[bad] <- NA_integer_
      if ("access_time" %in% names(df)) {
        df$access_time[bad] <- NA_integer_;   df$wait_time[bad] <- NA_integer_
        df$ride_time[bad] <- NA_integer_;     df$transfer_time[bad] <- NA_integer_
        df$egress_time[bad] <- NA_integer_;   df$n_rides[bad] <- NA_integer_
      }
    }
  }
  df
}

#' Finalize a completed scenario DB: checkpoint, verify, ready for archive/move.
#' @export
ttm_finalize <- function(db_path, scenario_id) {
  db_finalize(db_path, scenario_id, data_table = "ttm")
}

#' @export
expttm_finalize <- function(db_path, scenario_id) {
  db_finalize(db_path, scenario_id, data_table = "expttm_chunk")
}

#' @keywords internal
db_finalize <- function(db_path, scenario_id, data_table) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)")
  n_data <- DBI::dbGetQuery(con, sprintf(
    "SELECT count(DISTINCT origin_id) AS n FROM %s WHERE scenario_id = ?", data_table),
    params = list(scenario_id))$n
  n_done <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM done WHERE scenario_id = ?",
                            params = list(scenario_id))$n
  if (n_data != n_done) stop(sprintf("count mismatch: %s=%d done=%d",
                                     data_table, n_data, n_done))

  # completeness vs the frozen origin universe (review #5): matching data/done
  # counts alone would pass a half-finished scenario.
  n_expected <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM origin_index")$n
  if (n_done != n_expected)
    stop(sprintf("scenario incomplete: done=%d expected=%d", n_done, n_expected))
  missing <- DBI::dbGetQuery(con,
    "SELECT grid_id FROM origin_index
     EXCEPT SELECT origin_id FROM done WHERE scenario_id = ?",
    params = list(scenario_id))
  if (nrow(missing) > 0)
    stop("scenario incomplete - missing origins (first shown): ",
         paste(utils::head(missing$grid_id, 5), collapse = ", "))

  qc <- DBI::dbGetQuery(con, "PRAGMA quick_check")[[1]]
  if (!identical(qc, "ok")) stop("quick_check failed: ", paste(qc, collapse = "; "))
  invisible(list(n_origins = n_done, quick_check = qc))
}
