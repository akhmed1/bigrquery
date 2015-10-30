#' Create a new load job.
#'
#' This is a low-level function that creates an load job. To wait until it is
#' finished, see \code{\link{load_exec}}
#'
#' @param source_table Source table,
#'   either as a string in the format used by BigQuery, or as a list with
#'   \code{project_id}, \code{dataset_id}, and \code{table_id} entries
#' @param project project name
#' @param destination_uris List of destination URIs
#' @param compression Compression type ("NONE", "GZIP")
#' @param destination_format Destination format
#' @family jobs
#' @return a job resource list, as documented at
#'   \url{https://developers.google.com/bigquery/docs/reference/v2/jobs}
#' @seealso API documentation for insert method:
#'   \url{https://developers.google.com/bigquery/docs/reference/v2/jobs/insert}
#' @export
#'
insert_load_job <- function(source_uris, destination_table, schema, project,
                            billing = project,
                            load_config) {
  assert_that(is.string(destination_table), is.string(project),
              is.string(billing))


  if (is.character(destination_table)) {
    destination_table <- parse_table(destination_table, project_id = project)
  }

  assert_that(is.string(destination_table$project_id),
                is.string(destination_table$dataset_id),
                is.string(destination_table$table_id))

  load_config$sourceFormat <- ifelse(is.null(load_config$sourceFormat),
                                    "CSV",
                                    load_config$sourceFormat
                                   )
  body <- list(
    configuration = list(
      load =
        c(
          list(
            sourceUris = source_uris,
            schema = list(
              fields = schema
            ),
            destinationTable = list(
              datasetId = destination_table$dataset_id,
              projectId = destination_table$project_id,
              tableId = destination_table$table_id
            )
          ),
          load_config
        )
    )
  )

  url <- sprintf("projects/%s/jobs", billing)

  bq_post(url, body)
}

#' Run a asynchronous load job and wait till it is done
#'
#' This is a high-level function that inserts an load job
#' (with \code{\link{insert_load_job}}), repeatedly checks the status (with
#' \code{\link{get_job}}) until it is complete, then returns
#'
#' @inheritParams insert_load_job
#' @seealso Google documentation describing asynchronous queries:
#'  \url{https://developers.google.com/bigquery/docs/queries#asyncqueries}
#'
#'  Google documentation for handling large results:
#'  \url{https://developers.google.com/bigquery/loading-data#largeloadresults}
#' @export
#' @examples
#' \dontrun{
#' project <- "fantastic-voyage-389" # put your project ID here
#' sql <- "SELECT year, month, day, weight_pounds FROM [publicdata:samples.natality] LIMIT 5"
#' load_exec(sql, project = project)
#' # Put the results in a table you own (which uses project by default)
#' load_exec(sql, project = project, destination_table = "my_dataset.results")
#' # Use a default dataset for the load
#' sql <- "SELECT year, month, day, weight_pounds FROM natality LIMIT 5"
#' load_exec(sql, project = project, default_dataset = "publicdata:samples")
#' }
load_exec <- function(source_uris, destination_table, schema, project,
                      billing = project, load_config) {

  job <- insert_load_job(source_uris, destination_table, schema, project,
                         billing = project,
                         load_config = load_config)
  job <- wait_for(job)

  if (job$status$state == "DONE")
    (job$load)
  else
    (job$status)
}

