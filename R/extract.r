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


#' Collect the data by exporting into Google Cloud Storage.
#'
#' This is a high-level function that utilizes \code{\link{extract_exec}}
#' in order to export BigQuery table into Google Cloud Storage bucket,
#' download the resulting .csv.gz files locally, and subsequently
#' parse these files into a local data.frame
#'
#' @param data Reference to the BigQuery table or query to be collected
#' @param gs_bucket Google Cloud Storage bucket to be
#'        used as a temporary storage for export
#' @param local_dir Local directory to be used as a temporary local
#'        storage for downloaded .csv.gz files
#' @param quiet if \code{FALSE}, prints informative status messages
#' @return the local data.frame containing the required data
#' @examples
#' \dontrun{
#' Sys.setenv(GS_TEMP_BUCKET = "<YOUR_GS_BUCKET>")
#'
#' df <-
#'   src_bigquery(project="<PROJECT>", dataset="<DATASET>") %>%
#'   tbl("<TABLE>") %>%
#'   collect_by_export()
#' }
#' @export
collect_by_export <- function(data,
                              gs_bucket = Sys.getenv("GS_TEMP_BUCKET"),
                              local_dir = tempdir(),
                              quiet = getOption("bigquery.quiet")) {

  is_quiet <- function(x) isTRUE(quiet)

  # Google Storage URL base
  GS_URL <- "https://storage.googleapis.com"
  GS_DOWNLOAD_ATTEMPTS <- 10L

  # Compute data into temp table
  temp_table <-
    tempfile(pattern = "export", tmpdir = "") %>%
    str_replace_all("/","")

  remote_df <- data %>%
    compute(name = temp_table, temporary = FALSE)

  # Extract temp table to GS
  proj <- remote_df$src$con$project
  dataset <- remote_df$src$con$dataset

  if (!is_quiet())
    cat(paste0("\nExtracting data into Google Cloud Storage bucket ",
               gs_bucket, " ...\n"))

  num_files <- extract_exec(
    paste0(proj,":",dataset,".", temp_table),
    project = proj,
    paste0("gs://", gs_bucket, "/", temp_table, "_*", ".csv.gz"),
    compression = "GZIP",
    destination_format = "CSV") %>%
    as.integer()

  # Find out all files
  files <- sprintf(paste0(temp_table, "_%012d", ".csv.gz"),
                   0:(num_files-1))

  # Download all files from GS into tempdir
  if (!is_quiet())
    cat(paste0("\nDownloading ",num_files," .csv.gz files from ",
               gs_bucket, " into local ", local_dir, "...\n"))

  lapply(files, function(f) {
    local_file <- paste0(local_dir, f)

    # Make several attempts to download
    for (i in 1:GS_DOWNLOAD_ATTEMPTS) {
      get_result <-
        try(GET(url = paste0(GS_URL,"/",gs_bucket, "/", f),
                write_disk(local_file, overwrite = TRUE),
                config(token = get_access_cred()),
                progress()),
            silent = TRUE)

      # No error. Proceed
      if (!inherits(get_result,'try-error')) break

      # There was an error.
      if (i >= GS_DOWNLOAD_ATTEMPTS) {
        stop("**ERROR: Couldn't download ", local_file,
             " after ", GS_DOWNLOAD_ATTEMPTS, " attempts")
      } else {
        Sys.sleep(30) # Wait 30 seconds. Repeat.
      }
    }

  })

  if (!is_quiet())
    cat(paste0("\nParsing ",num_files," local .csv.gz files from ",
               local_dir, " into a data.frame ...\n"))

  df <- lapply(files, function(f) {
    local_file <- paste0(local_dir, f)
    (read_csv(local_file))
  }) %>% bind_rows()

  # Erase the temp table
  delete_table(project = proj,
               dataset = dataset,
               table = temp_table)

  (df)
}
