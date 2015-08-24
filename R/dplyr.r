#' A bigquery data source.
#'
#' Use \code{src_bigquery} to connect to an existing bigquery dataset,
#' and \code{tbl} to connect to tables within that database.
#'
#' @param project project id or name
#' @param dataset dataset name
#' @param billing billing project, if different to \code{project}
#' @export
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' # To run this example, replace billing with the id of one of your projects
#' # set up for billing
#' pd <- src_bigquery("publicdata", "samples", billing = "465736758727")
#' pd %>% tbl("shakespeare")
#'
#' # With bigquery data, it's always a good idea to start by selecting
#' # only the variables you're interested in - this reduces the amount of
#' # data that needs to be scanned and hence decreases costs
#' natality <- pd %>%
#'   tbl("natality") %>%
#'   select(year:day, state, child_race, weight_pounds)
#' year_weights <- natality %>%
#'   group_by(year) %>%
#'   summarise(weight = mean(weight_pounds), n = n()) %>%
#'   collect()
#' plot(year_weights$year, year_weights$weight, type = "b")
#' }
src_bigquery <- function(project, dataset, billing = project) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("dplyr is required to use src_bigquery", call. = FALSE)
  }

  assert_that(is.string(project), is.string(dataset), is.string(billing))

  if (!require("bigrquery")) {
    stop("bigrquery package required to connect to bigquery db", call. = FALSE)
  }

  con <- structure(list(project = project, dataset = dataset,
    billing = billing), class = "bigquery")
  dplyr::src_sql("bigquery", con)
}

#' @export
#' @importFrom dplyr tbl
tbl.src_bigquery <- function(src, from, ...) {
  dplyr::tbl_sql("bigquery", src = src, from = from, ...)
}

#' @export
#' @importFrom dplyr copy_to
copy_to.src_bigquery <- function(dest, df, name = deparse(substitute(df)), ...) {
  job <- insert_upload_job(dest$con$project, dest$con$dataset, name, df,
    billing = dest$con$billing)
  wait_for(job)

  tbl(dest, name)
}

#' @export
#' @importFrom dplyr src_desc
src_desc.src_bigquery <- function(x) {
  paste0("bigquery [", x$con$project, ":", x$con$dataset, "]")
}

#' @export
#' @importFrom dplyr db_list_tables
db_list_tables.bigquery <- function(con) {
  list_tables(con$project, con$dataset)
}

#' @export
#' @importFrom dplyr db_has_table
db_has_table.bigquery <- function(con, table) {
  table %in% list_tables(con$project, con$dataset)
}

#' @export
#' @importFrom dplyr db_query_fields
db_query_fields.bigquery <- function(con, sql) {
  info <- get_table(con$project, con$dataset, sql)
  vapply(info$schema$fields, "[[", "name", FUN.VALUE = character(1))
}

#' @export
#' @importFrom dplyr db_query_rows
db_query_rows.bigquery <- function(con, sql) {
  browser()
  info <- get_table(con$project, con$dataset, sql)
  browser()
}

#' @export
dim.tbl_bigquery <- function(x) {
  p <- x$query$ncol()
  c(NA, p)
}

#' @export
#' @importFrom dplyr mutate_
mutate_.tbl_bigquery <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  input <- dplyr::partial_eval(dots, .data)

  .data$mutate <- TRUE

  new <- dplyr:::update.tbl_sql(.data, select = c(.data$select, input))

  # BigQuery requires a collapse after any mutate
  dplyr::collapse(new)
}

# SQL -------------------------------------------------------------------------

# ((Temporary fix before "cross_join" is incorporated into dplyr))
#' Cross-Joins two tbls together.
#' @param x,y tbls to join
#' @param copy If \code{x} and \code{y} are not from the same data source,
#'   and \code{copy} is \code{TRUE}, then \code{y} will be copied into the
#'   same src as \code{x}.  This allows you to join tables across srcs, but
#'   it is a potentially expensive operation so you must opt into it.
#' @param ... other parameters passed onto methods
#' @name join
#' @rdname join
#' @export
cross_join <- function(x, y, copy = FALSE, ...) {
  UseMethod("cross_join")
}

#' @export
#' @rdname join
cross_join.tbl_bigquery <- function(x, y, copy = FALSE, ...) {
  y <- dplyr:::auto_copy(x, y, copy)
  sql <- dplyr::sql_join(x$src$con, x, y, type = "cross")
  dplyr:::update.tbl_sql(tbl(x$src, sql), group_by = dplyr::groups(x))
}
# ((End of temporary fix))

#' @export
#' @importFrom dplyr sql_select
sql_select.bigquery <- function(con, select, from, where = NULL,
                                group_by = NULL, having = NULL,
                                order_by = NULL, limit = NULL,
                                offset = NULL, ...) {

  out <- vector("list", 8)
  names(out) <- c("select", "from", "where", "group_by", "having", "order_by",
                  "limit", "offset")

  assert_that(is.character(select), length(select) > 0L)
  out$select <- dplyr::build_sql("SELECT ",
                                 dplyr::escape(select, collapse = ", ", con = con))

  assert_that(is.character(from), length(from) == 1L)
  out$from <- dplyr::build_sql("FROM ", from, con = con)

  if (length(where) > 0L) {
    assert_that(is.character(where))
    out$where <- dplyr::build_sql("WHERE ",
                                  dplyr::escape(where, collapse = " AND ", con = con))
  }

  if (!is.null(group_by)) {
    assert_that(is.character(group_by), length(group_by) > 0L)
    out$group_by <- dplyr::build_sql("GROUP EACH BY ",
                                     dplyr::escape(group_by, collapse = ", ", con = con))
  }

  if (!is.null(having)) {
    assert_that(is.character(having), length(having) == 1L)
    out$having <- dplyr::build_sql("HAVING ",
                                   dplyr::escape(having, collapse = ", ", con = con))
  }

  if (!is.null(order_by)) {
    assert_that(is.character(order_by), length(order_by) > 0L)
    out$order_by <- dplyr::build_sql("ORDER BY ",
                                     dplyr::escape(order_by, collapse = ", ", con = con))
  }

  if (!is.null(limit)) {
    assert_that(is.integer(limit), length(limit) == 1L)
    out$limit <- dplyr::build_sql("LIMIT ", limit, con = con)
  }

  if (!is.null(offset)) {
    assert_that(is.integer(offset), length(offset) == 1L)
    out$offset <- dplyr::build_sql("OFFSET ", offset, con = con)
  }

  dplyr::escape(unname(dplyr:::compact(out)), collapse = "\n", parens = FALSE, con = con)
}


#' @export
#' @importFrom dplyr sql_set_op
sql_set_op.bigquery <- function(con, x, y, method) {
  if (method != "UNION") {
    stop("Set operations other than UNION are not supported by BigQuery", call. = FALSE)
  }

  var_list <- dplyr:::sql_vector(
    sql_escape_ident(con, x$select),
    collapse=",", parens = FALSE, con = con
  )

  sql <- dplyr::build_sql(
    'SELECT ', var_list, ' FROM ',
    '(',x$query$sql,'),',
    '(',y$query$sql,')',
    con = con
  )

  attr(sql, "vars") <- x$select
  sql
}

#' @export
#' @importFrom dplyr sql_subquery
sql_subquery.bigquery <- function(con, sql, name =  dplyr::unique_name(), ...) {
  if (dplyr::is.ident(sql)) return(sql)

  subq <- dplyr::build_sql("(", sql, ") AS ", dplyr::ident(name), con = con)

  # build_sql removes "vars" from SQL but we do need to have them
  # for subsequent use inside sql_join.bigquery.
  # so we put them back
  attr(subq, "vars") <- attr(sql, "vars")
  (subq)
}

#' @export
#' @importFrom dplyr sql_join
sql_join.bigquery <- function(con, x, y, type = "inner", by = NULL, ...) {
  join <- switch(type,
                 left = dplyr::sql("LEFT JOIN EACH"),
                 inner = dplyr::sql("INNER JOIN EACH"),
                 right = dplyr::sql("RIGHT JOIN EACH"),
                 full = dplyr::sql("FULL JOIN EACH"),
                 cross = dplyr::sql("CROSS JOIN"),
                 stop("Unknown join type:", type, call. = FALSE)
  )

  if (type == "cross") {
    by <- list()
  } else {
    by <- dplyr:::common_by(by, x, y)
  }

  # Ensure tables have unique names
  x_names <- dplyr:::auto_names(x$select)
  y_names <- dplyr:::auto_names(y$select)
  uniques <- dplyr:::unique_names(x_names, y_names, by$x[by$x == by$y], x_suffix="_x", y_suffix="_y")

  if (is.null(uniques)) {
    sel_vars <- c(x_names, y_names)
  } else {
    x <- dplyr:::update.tbl_sql(x, select = setNames(x$select, uniques$x))
    y <- dplyr:::update.tbl_sql(y, select = setNames(y$select, uniques$y))

    by$x <- unname(uniques$x[by$x])
    by$y <- unname(uniques$y[by$y])

    sel_vars <- unique(c(uniques$x, uniques$y))
  }

  name_left <- dplyr:::unique_name()
  name_right <- dplyr:::unique_name()

  if (type == "cross") {
    cond <- dplyr::build_sql("", con = con)

    non_join_vars <- sel_vars

    sql_var_list <- dplyr:::sql_vector( c(dplyr::sql_escape_ident(con, non_join_vars)),
                                        collapse=",", parens = FALSE, con = con )
    final_sel_vars <- non_join_vars
  } else {
    on <- dplyr:::sql_vector(
            paste0( dplyr::sql_escape_ident(con, paste0(name_left,".",by$x)),
                    " = ",
                    dplyr::sql_escape_ident(con, paste0(name_right,".",by$y))
                  ),
                  collapse = " AND ", parens = TRUE, con = con )

    cond <- dplyr::build_sql("ON ", on, con = con)

    join_vars <- paste0(name_left,".",by$x)
    names(join_vars) <- by$x

    non_join_vars <- setdiff(setdiff(sel_vars, by$x), by$y)

    sql_var_list <- dplyr:::sql_vector(
                              c(sql_escape_ident(con, join_vars),
                                sql_escape_ident(con, non_join_vars)),
                              collapse=",", parens = FALSE, con = con )

    final_sel_vars <- union(by$x, non_join_vars)
  }

  from <- dplyr::build_sql (
    'SELECT ', sql_var_list,' FROM ',
    sql_subquery(con, x$query$sql, name=name_left), "\n\n",
    join, " \n\n" ,
    sql_subquery(con, y$query$sql, name=name_right), "\n\n",
    cond, con = con
  )
  attr(from, "vars") <- lapply(final_sel_vars, as.name)

  from
}

#' @export
#' @importFrom dplyr query
query.bigquery <- function(con, sql, .vars) {
  assert_that(is.string(sql))

  BigQuery$new(con, sql(sql), .vars)
}

#' @export
#' @importFrom dplyr db_save_query
db_save_query.bigquery <- function(con, sql, name, temporary=TRUE, ...) {
  table <- parse_table(name)

  if (is.null(table$project_id)) table$project_id <- con$project
  if (is.null(table$dataset_id)) table$dataset_id <- con$dataset

  if ( table$table_id %in% list_tables(table$project_id, table$dataset_id) ) {
    delete_table( project = table$project_id,
                  dataset = table$dataset_id,
                  table = table$table_id )
  }

  query_exec(query = sql,
             project = con$project,
             default_dataset = con$dataset,
             destination_table = paste0(table$project_id,":",
                                        table$dataset_id,".",
                                        table$table_id),
             max_pages = 1, warn = FALSE
  )

  name
}

BigQuery <- R6::R6Class("BigQuery",
  private = list(
    .nrow = NULL,
    .vars = NULL
  ),
  public = list(
    con = NULL,
    sql = NULL,

    initialize = function(con, sql, vars) {
      self$con <- con
      self$sql <- sql
      private$.vars <- vars
    },

    print = function(...) {
      cat("<Query> ", self$sql, "\n", sep = "")
      print(self$con)
    },

    fetch = function(n = -1L) {
      job <- insert_query_job(self$sql, self$con$billing,
        default_dataset = paste0(self$con$project, ":", self$con$dataset))
      job <- wait_for(job)

      dest <- job$configuration$query$destinationTable
      list_tabledata(dest$projectId, dest$datasetId, dest$tableId)
    },

    fetch_paged = function(chunk_size = 1e4, callback) {
      job <- insert_query_job(self$sql, self$con$billing,
        default_dataset = paste0(self$con$project, ":", self$con$dataset))
      job <- wait_for(job)

      dest <- job$configuration$query$destinationTable
      list_tabledata_callback(dest$projectId, dest$datasetId, dest$tableId,
        callback, page_size = chunk_size)
    },

    vars = function() {
      private$.vars
    },

    nrow = function() {
      if (!is.null(private$.nrow)) return(private$.nrow)
      private$.nrow <- db_query_rows(self$con, self$sql)
      private$.nrow
    },

    ncol = function() {
      length(self$vars())
    }
  )
)


# SQL translation --------------------------------------------------------------

#' @export
#' @importFrom dplyr src_translate_env
src_translate_env.src_bigquery <- function(x) {
  dplyr::sql_variant(
    dplyr::sql_translator(.parent = dplyr::base_scalar,
      # Casting
      as.logical = dplyr::sql_prefix("boolean"),
      as.numeric = dplyr::sql_prefix("float"),
      as.double = dplyr::sql_prefix("float"),
      as.integer = dplyr::sql_prefix("integer"),
      as.character = dplyr::sql_prefix("string"),

      # Date/time
      Sys.date = dplyr::sql_prefix("current_date"),
      Sys.time = dplyr::sql_prefix("current_time"),

      # Regular expressions
      grepl = function(match, x) {
        sprintf("REGEXP_MATCH(%s, %s)", dplyr::escape(x), dplyr::escape(match))
      },
      gsub = function(match, replace, x) {
        sprintf("REGEXP_REPLACE(%s, %s, %s)", dplyr::escape(x),
          dplyr::escape(match), dplyr::escape(replace))
      },

      # Other scalar functions
      ifelse = dplyr::sql_prefix("IF"),

      # stringr equivalents
      str_detect = function(x, match) {
        sprintf("REGEXP_MATCH(%s, %s)", dplyr::escape(x),
          dplyr::escape(match))
      },
      str_extract = function(x, match) {
        sprintf("REGEXP_EXTRACT(%s, %s)",  dplyr::escape(x),
          dplyr::escape(match))
      },
      str_replace = function(x, match, replace) {
        sprintf("REGEXP_REPLACE(%s, %s, %s)", dplyr::escape(x),
          dplyr::escape(match), dplyr::escape(replace))
      }
    ),
    dplyr::sql_translator(.parent = dplyr::base_agg,
      n = function() dplyr::sql("count(*)"),
      "%||%" = dplyr::sql_prefix("concat"),
      n_distinct = function(x) dplyr::build_sql("count(distinct(",x,"))"),
      sd =  dplyr::sql_prefix("stddev")
    ),
    dplyr::sql_translator(.parent = dplyr::base_win,
      mean  = function(...) stop("Not supported by bigquery"),
      sum   = function(...) stop("Not supported by bigquery"),
      min   = function(...) stop("Not supported by bigquery"),
      max   = function(...) stop("Not supported by bigquery"),
      n     = function(...) stop("Not supported by bigquery"),
      cummean = win_bq("mean"),
      cumsum  = win_bq("sum"),
      cummin  = win_bq("min"),
      cummax  = win_bq("max")
    )
  )
}

# BQ doesn't need frame clause
win_bq <- function(f) {
  force(f)
  function(x) {
    dplyr:::over(
      dplyr::build_sql(dplyr::sql(f), list(x)),
      dplyr:::partition_group(),
      dplyr:::partition_order()
    )
  }
}

#' @export
#' @importFrom dplyr sql_escape_string
sql_escape_string.bigquery <- function(con, x) {
  encodeString(x, na.encode = FALSE, quote = '"')
}

#' @export
#' @importFrom dplyr sql_escape_ident
sql_escape_ident.bigquery <- function(con, x) {
  y <- paste0("[", x, "]")
  y[is.na(x)] <- "NULL"
  names(y) <- names(x)

  y
}

