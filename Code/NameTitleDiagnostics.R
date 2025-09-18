#Diagnostics
# =========================================
# Diagnostics: one URL -> structured report
# =========================================
suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(stringr); library(httr2); library(xml2); library(rvest); library(tokenizers)
})

diagnose_leadership_page <- function(
    url,
    out_dir                = "E:/MonitorWebsitesR code/Output",
    prefix                 = "leaders",
    obey_robots            = TRUE,
    # harvest controls (mirror your function defaults)
    heading_levels         = c("h2","h3"),
    keywords               = c("leadership","executive","senior leadership","executive team",
                               "board","board of directors","governors","administration","management"),
    title_variants         = TITLE_LEXICON,
    include_headings       = TRUE,
    include_subheadings    = TRUE,
    include_h4_in_span     = TRUE,
    include_name_heading   = TRUE,
    synth_name_title_line  = TRUE,
    min_chars              = 120,
    # extraction controls
    min_confidence         = 1L,
    window_sentences       = 2L,
    # advanced: JS rendering toggle (if you add chromote later)
    use_chromote           = FALSE
) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  # 1) Lightweight HEAD/GET to get status & content-type
  req0 <- httr2::request(url) |> httr2::req_user_agent("diag/1.0")
  resp0 <- try(httr2::req_perform(req0), silent = TRUE)
  http_status <- if (inherits(resp0, "try-error")) NA_integer_ else httr2::resp_status(resp0)
  ctype <- if (inherits(resp0, "try-error")) NA_character_ else httr2::resp_headers(resp0)[["content-type"]]
  
  # 2) Run your heading-aware fetcher (writes .txt)
  #    Temporarily override its harvest internals by injecting parameters via a local wrapper
  fetch_local <- function() {
    # Copy of fetch that lets us pass harvest args
    req <- httr2::request(url) |>
      httr2::req_user_agent("diag/1.0") |>
      httr2::req_headers(Accept="text/html,*/*;q=0.8") |>
      httr2::req_timeout(30)
    resp <- try(httr2::req_perform(req), silent = TRUE)
    if (inherits(resp, "try-error")) {
      return(list(ok=FALSE, message=paste("Fetch failed:", attr(resp,"condition")$message)))
    }
    if (httr2::resp_status(resp) >= 400) {
      return(list(ok=FALSE, message=paste("HTTP", httr2::resp_status(resp))))
    }
    raw <- httr2::resp_body_string(resp)
    html_bytes <- nchar(raw, type="bytes")
    doc <- try(xml2::read_html(raw), silent = TRUE)
    if (inherits(doc,"try-error")) {
      return(list(ok=FALSE, message="HTML parse error"))
    }
    
    # Count headings overall & matched
    all_h <- xml2::xml_find_all(doc, ".//h1|.//h2|.//h3|.//h4")
    n_h1 <- sum(tolower(xml2::xml_name(all_h))=="h1")
    n_h2 <- sum(tolower(xml2::xml_name(all_h))=="h2")
    n_h3 <- sum(tolower(xml2::xml_name(all_h))=="h3")
    n_h4 <- sum(tolower(xml2::xml_name(all_h))=="h4")
    htxt <- tolower(rvest::html_text2(all_h))
    matched <- htxt[
      tolower(xml2::xml_name(all_h)) %in% heading_levels &
        (stringr::str_detect(htxt, paste0("\\b(", paste(stringr::str_replace_all(keywords, "\\+", "\\\\+"), collapse="|"), ")\\b")) |
           stringr::str_detect(htxt, paste0("\\b(", paste(stringr::str_replace_all(title_variants, "\\+", "\\\\+"), collapse="|"), ")\\b")) |
           stringr::str_detect(htxt, TITLE_REGEX))
    ]
    n_matched <- length(matched)
    
    # Use your harvest with name/title support
    section_txt <- harvest_sections_by_headings(
      doc,
      heading_levels, keywords, title_variants,
      include_headings, include_subheadings, include_h4_in_span,
      include_name_heading, synth_name_title_line, min_chars
    )
    
    # Save artifacts if we got anything
    file_txt <- NA_character_
    if (nzchar(section_txt)) {
      host <- urltools::url_parse(url)$domain %||% "site"
      path_bits <- urltools::url_parse(url)$path %||% ""
      stem <- paste0(safe(host), "_", safe(path_bits)); if (!nzchar(stem)) stem <- safe(host)
      stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
      fname <- glue::glue("diag_{stem}_{stamp}.txt") |> safe()
      file_txt <- file.path(out_dir, fname)
      writeLines(section_txt, file_txt, useBytes = TRUE)
    }
    
    list(
      ok = nzchar(section_txt),
      message = if (nzchar(section_txt)) "ok" else "no section text",
      html_bytes = html_bytes,
      n_h1 = n_h1, n_h2 = n_h2, n_h3 = n_h3, n_h4 = n_h4,
      n_matched = n_matched,
      section_chars = nchar(section_txt),
      section_preview = substr(section_txt, 1, 240),
      file_txt = file_txt,
      text = section_txt
    )
  }
  
  f <- fetch_local()
  
  # 3) NLP diagnostics (only if we got text)
  ner_counts <- NA
  title_hits <- NA_integer_
  cand_n <- NA_integer_
  final_n <- NA_integer_
  err_nlp <- NA_character_
  if (isTRUE(f$ok) && nzchar(f$text)) {
    # Title regex hits
    title_hits <- sum(stringr::str_detect(tokenizers::tokenize_sentences(f$text)[[1]], TITLE_REGEX))
    
    # NER
    try({
      init_spacy_safe()
      parsed <- spacyr::spacy_parse(f$text, entity = TRUE)
      ents <- spacyr::entity_extract(parsed, type = "named")
      ner_counts <- as.list(sort(table(ents$entity_type), decreasing = TRUE))
      # Candidates and final hits with loose thresholds
      hits <- extract_name_title_org(f$text, min_confidence = min_confidence, window_sentences = window_sentences)
      cand_n <- nrow(hits)
      final_n <- cand_n
    }, silent = TRUE) -> nlp_try
    
    if (inherits(nlp_try, "try-error")) {
      err_nlp <- paste("NLP error:", attr(nlp_try, "condition")$message)
    }
  }
  
  tibble::tibble(
    url = url,
    robots_checked = obey_robots,
    http_status = http_status,
    content_type = ctype %||% "",
    html_bytes = f$html_bytes %||% NA_integer_,
    h1 = f$n_h1 %||% NA_integer_,
    h2 = f$n_h2 %||% NA_integer_,
    h3 = f$n_h3 %||% NA_integer_,
    h4 = f$n_h4 %||% NA_integer_,
    matched_headings = f$n_matched %||% NA_integer_,
    section_chars = f$section_chars %||% 0L,
    saved_txt = f$file_txt %||% NA_character_,
    title_regex_hits = title_hits %||% NA_integer_,
    ner_PERSON = if (is.list(ner_counts)) (ner_counts$PERSON %||% 0L) else NA_integer_,
    ner_ORG    = if (is.list(ner_counts)) (ner_counts$ORG %||% 0L) else NA_integer_,
    final_hits = final_n %||% 0L,
    nlp_error  = err_nlp %||% ""
  )
}
#working URLs
#url <- "https://www.oakvalleyhealth.ca/about-us/meet-our-team/senior-leadership-team/" # Works
# url<-"https://sunnybrook.ca/content/?page=executive-leadership"
#url<- "https://www.stevensonhospital.ca/senior-leadership "
#  Partial Failure
#url<-"https://www.rvh.on.ca/about-rvh/senior-leadership-team/"

# url<-"https://www.sinaihealth.ca/about-sinai-health/sinai-health-leadership"
# Completely Failing URLS


#url<-"https://www.shn.ca/about-us/hospital-leadership/"
#url<-"https://www.sickkids.ca/en/about/leadership/"  # fails Try Chromote variant(Chatgpt)


#url<-"https://www.uhn.ca/corporate/AboutUHN/Governance_Leadership/Pages/Our_Leaders.aspx"


diag<-diagnose_leadership_page(url)
print(diag,width=Inf)
