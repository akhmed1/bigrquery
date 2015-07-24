#' Create a new extract job.
#'
#' This is a low-level function that creates an extract job. To wait until it is
#' finished, see \code{\link{extract_exec}}
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
insert_extract_job <- function(source_table, project, destination_uris,
                               compression = "NONE", destination_format = "CSV") {
  assert_that(is.string(project), is.string(source_table))

  if (!is.null(source_table)) {
    if (is.character(source_table)) {
      source_table <- parse_table(source_table, project_id = project)
    }
    assert_that(is.string(source_table$project_id),
                is.string(source_table$dataset_id),
                is.string(source_table$table_id))
  }

  url <- sprintf("projects/%s/jobs", project)
  body <- list(
    configuration = list(
      extract = list(
        sourceTable = list(
          datasetId = source_table$dataset_id,
          projectId = source_table$project_id,
          tableId = source_table$table_id
        ),
        destinationUris = destination_uris,
        compression = compression
      )
    )
  )

  bq_post(url, body)
}


#' Run a asynchronous extract job and wait till it is done
#'
#' This is a high-level function that inserts an extract job
#' (with \code{\link{insert_extract_job}}), repeatedly checks the status (with
#' \code{\link{get_job}}) until it is complete, then returns
#'
#' @inheritParams insert_extract_job
#' @seealso Google documentation describing asynchronous queries:
#'  \url{https://developers.google.com/bigquery/docs/queries#asyncqueries}
#'
#'  Google documentation for handling large results:
#'  \url{https://developers.google.com/bigquery/extracting-data#largeextractresults}
#' @export
#' @examples
#' \dontrun{
#' project <- "fantastic-voyage-389" # put your project ID here
#' sql <- "SELECT year, month, day, weight_pounds FROM [publicdata:samples.natality] LIMIT 5"
#' extract_exec(sql, project = project)
#' # Put the results in a table you own (which uses project by default)
#' extract_exec(sql, project = project, destination_table = "my_dataset.results")
#' # Use a default dataset for the extract
#' sql <- "SELECT year, month, day, weight_pounds FROM natality LIMIT 5"
#' extract_exec(sql, project = project, default_dataset = "publicdata:samples")
#' }
extract_exec <- function(source_table, project, destination_uris,
                         compression = "NONE", destination_format = "CSV") {
  assert_that(is.string(source_table), is.string(project))

  job <- insert_extract_job(source_table, project, destination_uris,
                            compression, destination_format)
  job <- wait_for(job)

  if(job$status$state == "DONE") {
    (job$statistics$extract$destinationUriFileCounts)
  } else {
    (0)
  }
}
