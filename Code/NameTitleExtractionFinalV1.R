# ============================================================
# Hospital Leadership Scraper + NLP Extractor (R + spacyr)
# - Robots-aware fetch
# - Heading-aware harvest (includes H2/H3/H4 text in output)
# - Saves .txt and extracts ALL NAME–TITLE–ORG candidates
# Env: condaenv="myenv", model="en_core_web_md"
# ============================================================

suppressPackageStartupMessages({
  library(robotstxt)
  library(httr2)
  library(xml2)
  library(rvest)
  library(stringr)
  library(fs)
  library(urltools)
  library(glue)
  library(purrr)
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(tokenizers)
  library(spacyr)
})

# ---------- Config ----------
.SPACY_CONDAENV <- "myenv"
.SPACY_MODEL    <- "en_core_web_md"  # ensure: conda activate myenv; python -m spacy download en_core_web_md

# ---------- Small utils ----------
`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (is.character(a) && !nzchar(a)) return(b)
  a
}
ua_str <- function() glue("Mozilla/5.0 (compatible; R/{getRversion()}; +https://www.r-project.org/) hospital-leadership/1.1")
safe <- function(x) {
  x <- stringr::str_replace_all(x, "[^[:alnum:]._\\-]+", "_")
  x <- stringr::str_replace_all(x, "_+", "_")
  x <- stringr::str_remove(x, "^_+")
  x <- stringr::str_remove(x, "_+$")
  x
}
normalize_text <- function(x) {
  x <- stringr::str_replace_all(x, "\\r\\n|\\r", "\n")
  x <- stringr::str_replace_all(x, "[ \\t]+", " ")
  x <- stringr::str_replace_all(x, "\n{3,}", "\n\n")
  stringr::str_squish(x)
}
is_pdf_url     <- function(url) stringr::str_detect(tolower(url), "\\.pdf(\\?.*)?$")
is_pdf_content <- function(resp) {
  ct <- httr2::resp_headers(resp)[["content-type"]] %||% ""
  stringr::str_detect(ct, "application/pdf")
}

# ---------- Title lexicon & regex ----------
TITLE_LEXICON <- c(
  # Exec core
  "chief executive officer","president and chief executive officer",
  "president & chief executive officer","president and ceo","president & ceo","president",
  "executive director","managing director",
  "chief operating officer","chief financial officer","chief medical officer",
  "chief information officer","chief people officer",
  "ceo","coo","cfo","cmo","cio","cpo",
  "general manager","gm",
  # Health/governance flavors
  "board chair","chair of the board","chair",
  "chief nursing executive","chief nurse executive","chief of staff",
  "medical director","vice president","vp","senior vice president","svp",
  "vp clinical services","vice president, clinical services",
  "chief digital officer","chief technology officer","cto",
  "chief people and culture officer","chief people & culture officer"
)

re_escape <- function(x) {
  stringr::str_replace_all(x, "([.^$|()\\[\\]{}*+?\\\\])", "\\\\\\1")
}
build_title_regex <- function(lex) {
  lex2 <- lex[order(nchar(lex), decreasing = TRUE)]
  esc  <- re_escape(lex2)
  paste0("(?i)\\b(", paste(esc, collapse = "|"), ")\\b")
}
TITLE_REGEX <- build_title_regex(TITLE_LEXICON)

# ---------- spaCy init (robust probe) ----------
init_spacy_safe <- function(model = .SPACY_MODEL, condaenv = .SPACY_CONDAENV, python_exe = NULL) {
  probe <- try(spacyr::spacy_parse("ping", entity = FALSE), silent = TRUE)
  if (inherits(probe, "try-error")) {
    tryCatch({
      if (!is.null(python_exe)) {
        spacyr::spacy_initialize(python_executable = python_exe, model = model)
      } else {
        spacyr::spacy_initialize(condaenv = condaenv, model = model)
      }
    }, error = function(e) {
      message("⚠️ spaCy not available in env '", condaenv, "'. In terminal run:\n",
              "   conda activate ", condaenv, "\n",
              "   pip install -U spacy\n",
              "   python -m spacy download ", model)
      stop(e)
    })
  }
}

# ---------- Heading-aware section harvest (INCLUDES H2/H3/H4 lines) ----------

harvest_sections_by_headings <- function(
    doc,
    heading_levels       = c("h2","h3"),
    keywords             = c("leadership","executive","senior leadership","executive team",
                             "board","board of directors","governors","administration","management"),
    title_variants       = TITLE_LEXICON,   # reuses your global title list
    include_headings     = TRUE,
    include_subheadings  = TRUE,
    include_h4_in_span   = TRUE,
    include_name_heading = TRUE,            # NEW: pull name from a preceding heading
    synth_name_title_line= TRUE,            # NEW: inject "Name, Title" helper line
    min_chars            = 120
) {
  body <- xml2::xml_find_first(doc, ".//body")
  if (is.na(body)) return("")
  
  headings_all <- xml2::xml_find_all(body, ".//h1|.//h2|.//h3|.//h4")
  if (length(headings_all) == 0) return("")
  htxt <- tolower(rvest::html_text2(headings_all))
  htag <- tolower(xml2::xml_name(headings_all))
  
  # Build matchers
  pat_kw    <- paste0("\\b(", paste(stringr::str_replace_all(keywords, "\\+", "\\\\+"), collapse="|"), ")\\b")
  pat_title <- paste0("\\b(", paste(stringr::str_replace_all(title_variants, "\\+", "\\\\+"), collapse="|"), ")\\b")
  # Person-ish heading heuristic (handles hyphens, apostrophes, middle initials)
  person_like <- function(s) {
    s <- stringr::str_squish(s)
    s <- stringr::str_replace_all(s, "\\.", ".") # normalize
    # e.g., "Jane A. Doe", "Mary-Jo O'Neil", "José M. De Souza"
    grepl("^[A-Z][A-Za-z'\\-]+(?:\\s+[A-Z]\\.)?(?:\\s+[A-Z][A-Za-z'\\-]+){1,3}$", s)
  }
  
  # Markdown heading prefix
  md_prefix <- function(tag) {
    tag <- tolower(tag)
    if (tag == "h1") return("# ")
    if (tag == "h2") return("## ")
    if (tag == "h3") return("### ")
    if (tag == "h4") return("#### ")
    "## "
  }
  
  # Title matcher using your TITLE_REGEX
  is_title_text <- function(x) {
    # TITLE_REGEX is already case-insensitive with (?i)
    any(stringr::str_detect(x, TITLE_REGEX))
  }
  
  # Start points: H2/H3 that look like leadership/title sections
  target_idx <- which(htag %in% heading_levels &
                        (stringr::str_detect(htxt, pat_kw) | stringr::str_detect(htxt, pat_title) | is_title_text(htxt)))
  if (length(target_idx) == 0) {
    target_idx <- which(htag %in% heading_levels &
                          stringr::str_detect(htxt, "(lead|exec|board|govern|senior|team)"))
    if (length(target_idx) == 0) return("")
  }
  
  out_sections <- purrr::map_chr(target_idx, function(i) {
    start <- headings_all[[i]]
    start_tag <- xml2::xml_name(start)
    start_txt <- rvest::html_text2(start)
    current_level <- as.integer(stringr::str_sub(tolower(start_tag), 2, 2))
    
    # Find a preceding NAME heading (same parent), if applicable
    name_heading_txt <- NULL
    if (include_name_heading) {
      parent   <- xml2::xml_parent(start)
      siblings <- xml2::xml_children(parent)
      idx      <- which(vapply(siblings, identical, logical(1), y = start))
      if (length(idx) == 1 && idx > 1) {
        # scan backwards to previous heading node
        for (k in seq.int(idx - 1, 1)) {
          nm <- tolower(xml2::xml_name(siblings[[k]]))
          if (grepl("^h[1-4]$", nm)) {
            cand <- rvest::html_text2(siblings[[k]]) |> stringr::str_squish()
            # treat as NAME if it "looks like" a person and is not itself a title
            if (person_like(cand) && !is_title_text(cand)) {
              name_heading_txt <- cand
            }
            break
          }
        }
      }
    }
    
    # Determine the slice of content for this section (until next <= level)
    parent <- xml2::xml_parent(start)
    siblings <- xml2::xml_children(parent)
    sib_idx <- which(vapply(siblings, identical, logical(1), y = start))
    if (length(sib_idx) == 0) return("")
    
    stop_idx <- length(siblings) + 1
    if (sib_idx < length(siblings)) {
      for (j in (sib_idx + 1):length(siblings)) {
        nm <- tolower(xml2::xml_name(siblings[[j]]))
        if (grepl("^h[1-4]$", nm)) {
          lvl <- as.integer(stringr::str_sub(nm, 2, 2))
          if (lvl <= current_level || (!include_h4_in_span && lvl == 4)) {
            stop_idx <- j
            break
          }
        }
      }
    }
    
    # If there is no body, still emit headings so the extractor can use them
    if (sib_idx + 1 >= stop_idx) {
      parts <- c(
        if (!is.null(name_heading_txt)) paste0("## ", name_heading_txt) else NULL,
        paste0(md_prefix(start_tag), stringr::str_squish(start_txt))
      )
      return(paste(parts, collapse = "\n"))
    }
    
    nodes <- siblings[(sib_idx + 1):(stop_idx - 1)]
    # prune chrome
    xml2::xml_find_all(nodes, ".//script|.//style|.//noscript|.//nav|.//footer|.//form|.//header") |> xml2::xml_remove()
    
    # optional subheadings inside the span
    subheading_lines <- character(0)
    if (include_subheadings) {
      subs <- xml2::xml_find_all(nodes, ".//h2|.//h3|.//h4")
      if (length(subs) > 0) {
        subheading_lines <- vapply(subs, function(h) {
          paste0(md_prefix(xml2::xml_name(h)), stringr::str_squish(rvest::html_text2(h)))
        }, character(1))
      }
    }
    
    body_txt <- paste(vapply(nodes, rvest::html_text2, character(1)), collapse = "\n") |>
      normalize_text()
    
    # Compose output:
    # - Optional NAME heading
    # - Current (title) heading
    # - Optional synthesized "Name, Title" helper line (great for NER)
    # - Subheadings and body text
    synth_line <- NULL
    if (synth_name_title_line && !is.null(name_heading_txt) && is_title_text(start_txt)) {
      synth_line <- paste0(name_heading_txt, ", ", stringr::str_squish(start_txt))
    }
    
    parts <- c(
      if (!is.null(name_heading_txt)) paste0("## ", name_heading_txt) else NULL,
      if (include_headings) paste0(md_prefix(start_tag), stringr::str_squish(start_txt)) else NULL,
      synth_line,
      if (length(subheading_lines) > 0) paste(subheading_lines, collapse = "\n") else NULL,
      body_txt
    )
    section <- paste(parts, collapse = "\n")
    section <- stringr::str_trim(section)
    
    if (nchar(section) < min_chars) "" else section
  })
  
  out_sections <- out_sections[nchar(out_sections) > 0]
  paste(unique(out_sections), collapse = "\n\n--- SECTION BREAK ---\n\n")
}


# ---------- Fetch + write .txt using heading-aware harvest ----------
fetch_leadership_to_textfile <- function(url,
                                         out_dir = "data/raw",
                                         prefix = "leaders",
                                         obey_robots = TRUE,
                                         max_attempts = 3,
                                         timeout_sec = 30) {
  stopifnot(is.character(url), length(url) == 1)
  fs::dir_create(out_dir)
  
  # robots
  if (obey_robots) {
    host <- urltools::domain(url)
    ok <- try(robotstxt::paths_allowed(paths = url, bot = "hospital-leadership", domain = host), silent = TRUE)
    if (inherits(ok, "try-error") || isFALSE(ok)) {
      msg <- glue("Blocked by robots.txt or robots check failed for: {url}")
      warning(msg)
      return(list(text = "", file = NA_character_, ok = FALSE, message = msg))
    }
  }
  if (is_pdf_url(url)) {
    msg <- "PDF URL detected; use your earlier PDF-capable fetcher."
    warning(msg)
    return(list(text = "", file = NA_character_, ok = FALSE, message = msg))
  }
  
  # HTTP fetch
  req <- httr2::request(url) |>
    httr2::req_user_agent(ua_str()) |>
    httr2::req_timeout(timeout_sec) |>
    httr2::req_headers(Accept = "text/html,*/*;q=0.8")
  resp <- NULL
  for (i in seq_len(max_attempts)) {
    resp <- try(httr2::req_perform(req), silent = TRUE)
    if (!inherits(resp, "try-error") && httr2::resp_status(resp) < 500) break
    Sys.sleep(1.25 * i)
  }
  if (inherits(resp, "try-error")) {
    msg <- glue("Fetch failed: {attr(resp, 'condition')$message}")
    warning(msg)
    return(list(text = "", file = NA_character_, ok = FALSE, message = msg))
  }
  if (httr2::resp_status(resp) >= 400 || is_pdf_content(resp)) {
    msg <- glue("HTTP {httr2::resp_status(resp)} or PDF content at {url}")
    warning(msg)
    return(list(text = "", file = NA_character_, ok = FALSE, message = msg))
  }
  
  doc <- try(xml2::read_html(httr2::resp_body_string(resp)), silent = TRUE)
  if (inherits(doc, "try-error")) {
    msg <- "HTML parse error."
    warning(msg)
    return(list(text = "", file = NA_character_, ok = FALSE, message = msg))
  }
  
  section_txt <- harvest_sections_by_headings(doc)
  if (!nzchar(section_txt)) {
    # fallback: main/article/body
    xml2::xml_find_all(doc, ".//script|.//style|.//noscript|.//nav|.//footer|.//form|.//header") |> xml2::xml_remove()
    node <- rvest::html_element(doc, "main")
    if (is.na(node)) node <- rvest::html_element(doc, "[role='main']")
    if (is.na(node)) node <- rvest::html_element(doc, "article")
    if (is.na(node)) node <- rvest::html_element(doc, "body")
    section_txt <- normalize_text(rvest::html_text2(node))
  }
  if (!nzchar(section_txt)) {
    msg <- "No visible text extracted (even with fallback)."
    warning(msg)
    return(list(text = "", file = NA_character_, ok = FALSE, message = msg))
  }
  
  host <- urltools::url_parse(url)$domain %||% "site"
  path_bits <- urltools::url_parse(url)$path %||% ""
  stem <- paste0(safe(host), "_", safe(path_bits))
  if (!nzchar(stem)) stem <- safe(host)
  stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
  fname <- glue("{prefix}_{stem}_{stamp}.txt")
  fname <- safe(fname)
  fpath <- file.path(out_dir, fname)
  
  writeLines(section_txt, fpath, useBytes = TRUE)
  list(text = section_txt, file = fpath, ok = TRUE, message = "ok")
}

# ---------- Pattern boosters ----------
PATTERN_NAME_COMMA_TITLE <- function(name, title_regex) {
  paste0("(?i)\\b", stringr::str_replace_all(name, "\\.", "\\\\."), "\\b\\s*,\\s*(?:the\\s+)?", title_regex)
}
PATTERN_TITLE_NAME <- function(name, title_regex) {
  paste0("(?i)", title_regex, "\\s+\\b", stringr::str_replace_all(name, "\\.", "\\\\."), "\\b")
}
PATTERN_NAME_IS_TITLE <- function(name, title_regex) {
  paste0("(?i)\\b", stringr::str_replace_all(name, "\\.", "\\\\."), "\\b\\s+is\\s+(?:the\\s+|a\\s+)?", title_regex)
}

# ---------- Extract ALL candidates ----------
extract_name_title_org <- function(text,
                                   min_confidence = 2L,
                                   window_sentences = 1L,
                                   model = .SPACY_MODEL,
                                   condaenv = .SPACY_CONDAENV) {
  stopifnot(is.character(text), length(text) == 1)
  init_spacy_safe(model, condaenv)
  
  sents <- tokenizers::tokenize_sentences(text)[[1]]
  if (length(sents) == 0) {
    return(tibble(NAME = character(), TITLE = character(), ORG = character(),
                  CONFIDENCE = integer(), SENTENCE = character()))
  }
  
  parsed <- spacyr::spacy_parse(text, entity = TRUE, nounphrase = FALSE)
  ents   <- spacyr::entity_extract(parsed, type = "named")
  if (nrow(ents) == 0) {
    return(tibble(NAME = character(), TITLE = character(), ORG = character(),
                  CONFIDENCE = integer(), SENTENCE = character()))
  }
  if ("sentence_id" %in% names(ents)) {
    ents <- dplyr::rename(ents, sent_id = sentence_id)
  }
  
  # PERSON/ORG per sentence via pivot_wider (fixed values_fill)
  ents_by_sent <- ents |>
    dplyr::transmute(sent_id, entity, entity_type) |>
    dplyr::group_by(sent_id, entity_type) |>
    dplyr::summarise(vals = list(unique(entity)), .groups = "drop") |>
    tidyr::pivot_wider(
      names_from  = entity_type,
      values_from = vals,
      values_fill = list(vals = list(character()))
    )
  
  titles_by_sent <- tibble(sent_id = seq_along(sents), SENTENCE = sents) |>
    dplyr::mutate(TITLE = purrr::map(SENTENCE, function(sent) {
      m <- stringr::str_extract_all(sent, TITLE_REGEX, simplify = FALSE)[[1]]
      unique(stringr::str_squish(m))
    }))
  
  merged <- titles_by_sent |>
    dplyr::left_join(ents_by_sent, by = "sent_id") |>
    dplyr::mutate(
      PERSON = purrr::map(PERSON, function(x) if (is.null(x)) character() else x),
      ORG    = purrr::map(ORG,    function(x) if (is.null(x)) character() else x),
      TITLE  = purrr::map(TITLE,  function(x) if (is.null(x)) character() else x)
    )
  
  cand_in_sent <- merged |>
    dplyr::filter(lengths(TITLE) + lengths(PERSON) + lengths(ORG) > 0) |>
    dplyr::mutate(combos = purrr::pmap(list(TITLE, PERSON, ORG, SENTENCE), function(tt, pp, oo, sent) {
      tt <- unique(tt); pp <- unique(pp); oo <- unique(oo)
      if (length(tt) == 0) tt <- NA_character_
      if (length(pp) == 0) pp <- NA_character_
      if (length(oo) == 0) oo <- NA_character_
      tidyr::expand_grid(TITLE = tt, NAME = pp, ORG = oo) |>
        dplyr::mutate(SENTENCE = sent)
    })) |>
    dplyr::select(sent_id, combos) |>
    tidyr::unnest(combos)
  
  pull_nearby <- function(idx, need_col) {
    rng <- max(1, idx - window_sentences):min(length(sents), idx + window_sentences)
    pool <- merged |> dplyr::filter(sent_id %in% rng)
    vec <- unique(unlist(pool[[need_col]]))
    if (length(vec) == 0) NA_character_ else vec
  }
  
  cands_aug <- cand_in_sent |>
    dplyr::rowwise() |>
    dplyr::mutate(
      TITLE = if (is.na(TITLE) || TITLE == "") {
        v <- pull_nearby(sent_id, "TITLE")
        if (length(v) > 1) v[1] else v
      } else TITLE,
      ORG = if (is.na(ORG) || ORG == "") {
        v <- pull_nearby(sent_id, "ORG")
        if (length(v) > 1) v[1] else v
      } else ORG
    ) |>
    dplyr::ungroup()
  
  score_row <- function(name, title, org, sentence) {
    score <- 0L
    if (!is.na(name)  && nzchar(name))  score <- score + 1L
    if (!is.na(title) && nzchar(title)) score <- score + 1L
    if (!is.na(org)   && nzchar(org))   score <- score + 1L
    
    if (!is.na(name) && nzchar(name) && !is.na(title) && nzchar(title)) {
      t_re <- paste0("(?i)\\b", stringr::str_replace_all(title, "\\.", "\\\\."), "\\b")
      if (stringr::str_detect(sentence, PATTERN_NAME_COMMA_TITLE(name, t_re))) score <- score + 2L
      if (stringr::str_detect(sentence, PATTERN_TITLE_NAME(name, t_re)))       score <- score + 2L
      if (stringr::str_detect(sentence, PATTERN_NAME_IS_TITLE(name, t_re)))    score <- score + 2L
    }
    if (!is.na(org) && nzchar(org) && !is.na(title) && nzchar(title)) {
      t_re <- paste0("(?i)\\b", stringr::str_replace_all(title, "\\.", "\\\\."), "\\b")
      o_re <- paste0("(?i)\\b", stringr::str_replace_all(org,   "\\.", "\\\\."), "\\b")
      if (stringr::str_detect(sentence, paste0(t_re, "\\s+(?:of|at)\\s+", o_re))) score <- score + 1L
    }
    score
  }
  
  results <- cands_aug |>
    dplyr::mutate(CONFIDENCE = purrr::pmap_int(list(NAME, TITLE, ORG, SENTENCE), score_row)) |>
    dplyr::arrange(dplyr::desc(CONFIDENCE), dplyr::desc(!is.na(NAME)), dplyr::desc(!is.na(TITLE))) |>
    dplyr::distinct(NAME, TITLE, ORG, SENTENCE, .keep_all = TRUE) |>
    dplyr::filter(CONFIDENCE >= min_confidence) |>
    dplyr::transmute(
      NAME = ifelse(is.na(NAME), "", NAME),
      TITLE = ifelse(is.na(TITLE), "", stringr::str_squish(TITLE)),
      ORG = ifelse(is.na(ORG), "", ORG),
      CONFIDENCE,
      SENTENCE = stringr::str_squish(SENTENCE)
    )
  
  results
}

# ---------- Example: fetch + extract ALL ----------

out_dir <- "E:/MonitorWebsitesR code/Output"
 dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
url <- "https://www.oakvalleyhealth.ca/about-us/meet-our-team/senior-leadership-team/"
#url<-"https://www.sickkids.ca/en/about/leadership/"
#url<-"https://www.sinaihealth.ca/about-sinai-health/sinai-health-leadership"
#url<-"https://sunnybrook.ca/content/?page=executive-leadership"
res <- fetch_leadership_to_textfile(url, out_dir = out_dir, prefix = "leaders")

if (isTRUE(res$ok)) {
   init_spacy_safe()
   all_execs <- extract_name_title_org(res$text, min_confidence = 1L, window_sentences = 2L)
   print(all_execs, n = Inf, width = Inf)
 } else {
   cat("Fetch failed:", res$message, "\n")
 }
