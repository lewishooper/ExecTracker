# ============================================================
# Batch runner
# - Reads YAML profiles (defaults + per-domain overrides)
# - Accepts: vector of URLs OR a data.frame with a URL column
# - For each URL: builds per-domain TITLE regex, harvests text (with fallback),
#   saves .txt, runs extractor with per-domain thresholds, writes CSV summary
# ============================================================

source("E:/ExecTracker/Code/hospital_leadership_scraper.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(tibble)
  library(fs)
  library(readr)   # if you want to read CSVs for the URL list
})
input_urls<-readRDS("E:/ExecTracker/source/urls.r")
# Source the scraper (adjust the path if needed)
# source("hospital_leadership_scraper.R")

normalize_url <- function(u) {
  u <- trimws(u)
  u <- sub("\\s+#.*$", "", u)
  u <- sub("/+$", "", u)
  u
}
#test   urls_or_df<-input_urls
# url_col<-"url_column"
# Accepts either a character vector of URLs or a data.frame with a URL column name
run_batch_with_profiles <- function(
    urls_or_df,
    url_col       = url_column,  # set if you pass a data.frame (e.g., "LeadershipURL")
    profiles_path = "E:/ExecTracker/config/hospital_profiles.yaml",
    out_dir       = "E:/ExecTracker/Outputs",
    csv_out       = file.path(out_dir, "leadership_hits.csv")
) {
  fs::dir_create(out_dir, recurse = TRUE)
  fs::dir_create(fs::path_dir(csv_out), recurse = TRUE)
# test line urls_or_df<-input_urls

  # Build the URL vector
  if (is.character(urls_or_df)) {
    urls <- urls_or_df
  } else if (is.data.frame(urls_or_df)) {
    if (is.null(url_col) || !(url_col %in% names(urls_or_df))) {
      stop("When passing a data.frame, provide url_col with the column name that contains URLs.")
    }
    urls <- urls_or_df[[url_col]]
  } else {
    stop("urls_or_df must be a character vector or data.frame.")
  }
  urls <- normalize_url(urls)
  urls <- urls[nzchar(urls) & grepl("^https?://", urls, ignore.case = TRUE)]

  
  profiles <- load_profiles(profiles_path)
  
  rows <- purrr::map(urls, function(u) {
    # Profile-aware fetch (saves .txt)
    f <- fetch_leadership_profiled(u, profiles, out_dir = out_dir, prefix = "leaders")
    if (!isTRUE(f$ok)) {
      return(tibble(
        url = u, file = NA_character_, ok = FALSE, message = f$message,
        NAME = "", TITLE = "", ORG = "", CONFIDENCE = 0L, SENTENCE = ""
      ))
    }
    
    # Use the profile thresholds and re-build TITLE regex for extraction (already done in fetch, but safe)
    p <- f$profile
    TITLES_RUN <- c(TITLE_LEXICON, p$title_variants_extra)
    TITLE_REGEX <<- build_title_regex(TITLES_RUN)
    
    init_spacy_safe()
    hits <- extract_name_title_org(
      f$text,
      min_confidence   = as.integer(p$min_confidence),
      window_sentences = as.integer(p$window_sentences)
    )
    
    if (nrow(hits) == 0) {
      tibble(
        url = u, file = f$file, ok = TRUE, message = "no hits",
        NAME = "", TITLE = "", ORG = "", CONFIDENCE = 0L, SENTENCE = ""
      )
    } else {
      dplyr::bind_cols(
        tibble(url = u, file = f$file, ok = TRUE, message = "ok"),
        hits
      )
    }
  })
  
  out <- dplyr::bind_rows(rows)
  utils::write.csv(out, csv_out, row.names = FALSE, fileEncoding = "UTF-8")
  out
}
## 
results <- run_batch_with_profiles(input_urls,url_col = "url_column")
# -------------------
# Example usage
# -------------------
# 1) If you already have a character vector of 20 URLs:
# urls <- c("https://...", "https://...", ...)  # length 20
# results <- run_batch_with_profiles(urls)

# 2) If your 20 URLs live in a CSV with column 'LeadershipURL':
# df <- readr::read_csv("E:/MonitorWebsitesR code/Input/hospitals.csv", show_col_types = FALSE)
# results <- run_batch_with_profiles(df, url_col = "LeadershipURL")

# print(results, n = Inf, width = Inf)
