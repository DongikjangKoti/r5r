# Direct-to-SQLite TTM output: R-side schema owner and orchestration helpers.
# Design document: R5_TTM_DB_구축전략.md (v6). The R side OWNS the schema;
# the Java sink (TtmSink.java) only validates it and fails fast.

TTM_SCHEMA_VERSION  <- "1"
TTM_PAYLOAD_VERSION <- 1L

# immutable fingerprint keys: resume is refused unless ALL of these exist and match
TTM_FP_KEYS <- c(
  "schema_version", "payload_version", "layout", "codec", "compression_level",
  "unit", "sentinel", "width", "mode", "scenario_id", "departure_datetime",
  "time_window_size", "max_trip_duration", "percentiles", "n_dest",
  "origin_hash", "dest_hash", "network_file",
  "r5r_version", "r5_version", "sqlite_jdbc_version"
)

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
ttm_validate_current_meta <- function(meta_list) {
  missing_run <- setdiff(TTM_FP_KEYS, names(meta_list))
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
ttm_validate_db_fingerprint <- function(con, meta_list) {
  old <- DBI::dbGetQuery(con, "SELECT k, v FROM meta")
  for (k in TTM_FP_KEYS) {
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
ttm_init_or_validate_db <- function(db_path, destinations_full, meta_list, scenario_id) {
  meta_list$dest_hash   <- ttm_fingerprint(destinations_full)
  meta_list$origin_hash <- meta_list$dest_hash   # v1: square only, same set by definition
  meta_list$scenario_id <- as.character(scenario_id)

  ttm_validate_current_meta(meta_list)           # BEFORE the is_new branch

  is_new <- !file.exists(db_path) || file.size(db_path) == 0  # 0-byte file = empty DB
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (is_new) {                                  # state A
    DBI::dbWithTransaction(con, {
      ttm_create_schema(con)
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
  if (!all(TTM_REQUIRED_TABLES %in% tabs)) {
    stop("incomplete schema in existing DB: ",
         paste(setdiff(TTM_REQUIRED_TABLES, tabs), collapse = ", "),
         call. = FALSE)
  }

  ttm_validate_db_fingerprint(con, meta_list)    # states B/C; never re-initialize
  invisible("validated")
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

#' Finalize a completed scenario DB: checkpoint, verify, ready for archive/move.
#' @export
ttm_finalize <- function(db_path, scenario_id) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)")
  n_ttm  <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM ttm  WHERE scenario_id = ?",
                            params = list(scenario_id))$n
  n_done <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM done WHERE scenario_id = ?",
                            params = list(scenario_id))$n
  if (n_ttm != n_done) stop(sprintf("count mismatch: ttm=%d done=%d", n_ttm, n_done))
  qc <- DBI::dbGetQuery(con, "PRAGMA quick_check")[[1]]
  if (!identical(qc, "ok")) stop("quick_check failed: ", paste(qc, collapse = "; "))
  invisible(list(n_origins = n_ttm, quick_check = qc))
}
