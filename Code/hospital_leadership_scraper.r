# ============================================================
# Hospital Leadership Scraper (profile-aware) + NLP extractor
# - Robots-friendly fetch (plain HTML)
# - Heading-aware harvest (includes H2/H3/H4 text + optional synthesized “Name, Title” line)
# - CSS card fallback to grab Name/Title from card grids
# - Per-domain overrides pulled from YAML profiles
# - Saves a UTF-8 .txt and returns harvested text + chosen profile
# - Extracts ALL NAME–TITLE–ORG candidates with spacyr
# ============================================================

suppressPackageStartupMessages({
  library(httr2)
  library(xml2)
  library(rvest)
  library(stringr)
  library(yaml)
  library(urltools)
  library(glue)
  library(purrr)
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(tokenizers)
  library(spacyr)
  library(fs)
})

# ---------- Config (spaCy env/model) ----------
.SPACY_CONDAENV <- "myenv"
.SPACY_MODEL    <- "en_core_web_md"  # ensure: conda activate myenv; python -m spacy download en_core_web_md

# ---------- Small utils ----------
`%||%` <- function(a, b) { if (is.null(a) || length(a) == 0 || (is.character(a) && !nzchar(a))) b else a }

safe <- function(x) {
  x <- str_replace_all(x, "[^[:alnum:]._\\-]+", "_")
  x <- str_replace_all(x, "_+", "_")
  x <- str_remove(x, "^_+")
  x <- str_remove(x, "_+$")
  x
}

normalize_text <- function(x) {
  x <- str_replace_all(x, "\\r\\n|\\r", "\n")
  x <- str_replace_all(x, "[ \\t]+", " ")
  x <- str_replace_all(x, "\n{3,}", "\n\n")
  str_squish(x)
}

# ---------- Profiles (YAML) ----------
list_merge <- function(base, override) {
  if (is.null(override)) return(base)
  for (nm in names(override)) {
    if (is.list(base[[nm]]) && is.list(override[[nm]])) base[[nm]] <- list_merge(base[[nm]], override[[nm]])
    else base[[nm]] <- override[[nm]]
  }
  base
}

load_profiles <- function(path = "config/hospital_profiles.yaml") {
  cfg <- yaml::read_yaml(path)
  if (is.null(cfg$defaults)) stop("profiles yaml missing 'defaults'")
  if (is.null(cfg$domains))  cfg$domains <- list()
  cfg
}

profile_for_url <- function(url, profiles) {
  dom <- urltools::url_parse(url)$domain
  list_merge(profiles$defaults, profiles$domains[[dom]])
}

# ---------- Global title lexicon & regex ----------
TITLE_LEXICON <- c(
  "chief executive officer","president and chief executive officer",
  "president & chief executive officer","president and ceo","president & ceo","president",
  "executive director","managing director",
  "chief operating officer","chief financial officer","chief medical officer",
  "chief information officer","chief people officer",
  "ceo","coo","cfo","cmo","cio","cpo",
  "board chair","chair of the board","chair",
  "chief nursing executive","chief nurse executive","chief of staff",
  "medical director","vice president","vp","senior vice president","svp",
  "vp clinical services","vice president, clinical services",
  "chief digital officer","chief technology officer","cto",
  "chief people and culture officer","chief people & culture officer"
)

re_escape <- function(x) str_replace_all(x, "([.^$|()\\[\\]{}*+?\\\\])", "\\\\\\1")
build_title_regex <- function(lex) {
  lex2 <- lex[order(nchar(lex), decreasing = TRUE)]
  paste0("(?i)\\b(", paste(re_escape(lex2), collapse = "|"), ")\\b")
}
# This global is rebuilt per URL based on the profile:
TITLE_REGEX <- build_title_regex(TITLE_LEXICON)

# ---------- spaCy init ----------
init_spacy_safe <- function(model = .SPACY_MODEL, condaenv = .SPACY_CONDAENV, python_exe = NULL) {
  probe <- try(spacyr::spacy_parse("ping", entity = FALSE), silent = TRUE)
  if (inherits(probe, "try-error")) {
    if (!is.null(python_exe)) {
      spacyr::spacy_initialize(python_executable = python_exe, model = model)
    } else {
      spacyr::spacy_initialize(condaenv = condaenv, model = model)
    }
  }
}

# ---------- Part A: CSS card fallback ----------
css_card_fallback <- function(doc,
                              min_cards = 3,
                              name_sel  = "h1, h2, h3, .name, .card-title, .profile-name, .person__name, .title--name",
                              title_sel = "h4, h5, .title, .role, .position, .card-subtitle, .profile-title, .person__role") {
  containers <- html_elements(doc,
                              "section, article, .content, .container, .grid, .cards, [class*='leadership'], [class*='team'], [class*='profile'], [class*='people']"
  )
  if (length(containers) == 0) return("")
  
  cards <- html_elements(containers,
                         ".card, .c-card, .card--person, .person, .profile, .team__member, .teaser, .tile, .grid__item, li, article"
  )
  if (length(cards) == 0) return("")
  
  rows <- lapply(cards, function(card) {
    nm <- html_text2(html_element(card, name_sel))
    tt <- html_text2(html_element(card, title_sel))
    nm <- str_squish(nm); tt <- str_squish(tt)
    if (nzchar(nm) || nzchar(tt)) list(name = nm, title = tt, body = html_text2(card)) else NULL
  })
  rows <- rows[!vapply(rows, is.null, logical(1))]
  if (length(rows) < min_cards) return("")
  
  parts <- vapply(rows, function(r) {
    nm <- r$name; tt <- r$title; body <- normalize_text(r$body)
    if (nchar(body) > 1000) body <- substr(body, 1, 1000)
    paste(
      if (nzchar(nm)) paste0("## ", nm) else NULL,
      if (nzchar(tt)) paste0("### ", tt) else NULL,
      if (nzchar(nm) && nzchar(tt)) paste0(nm, ", ", tt) else NULL,
      body,
      sep = "\n"
    )
  }, character(1))
  
  paste(parts, collapse = "\n\n--- CARD ---\n\n")
}

# ---------- Heading-aware harvest (includes Name + synthesized line) ----------
harvest_sections_by_headings <- function(
    doc,
    heading_levels       = c("h2","h3"),
    keywords             = c("leadership","executive","senior leadership","executive team",
                             "board","board of directors","governors","administration","management"),
    title_variants       = TITLE_LEXICON,
    include_headings     = TRUE,
    include_subheadings  = TRUE,
    include_h4_in_span   = TRUE,
    include_name_heading = TRUE,
    synth_name_title_line= TRUE,
    min_chars            = 120
) {
  body <- xml_find_first(doc, ".//body")
  if (is.na(body)) return("")
  
  headings_all <- xml_find_all(body, ".//h1|.//h2|.//h3|.//h4")
  if (length(headings_all) == 0) return("")
  htxt <- tolower(html_text2(headings_all))
  htag <- tolower(xml_name(headings_all))
  
  pat_kw    <- paste0("\\b(", paste(str_replace_all(keywords, "\\+", "\\\\+"), collapse="|"), ")\\b")
  pat_title <- paste0("\\b(", paste(str_replace_all(title_variants, "\\+", "\\\\+"), collapse="|"), ")\\b")
  
  # title helper
  is_title_text <- function(x) any(str_detect(x, TITLE_REGEX))
  
  # name-like heading
  person_like <- function(s) {
    s <- str_squish(s)
    grepl("^[A-Z][A-Za-z'\\-]+(?:\\s+[A-Z]\\.)?(?:\\s+[A-Z][A-Za-z'\\-]+){1,3}$", s)
  }
  
  target_idx <- which(htag %in% heading_levels &
                        (str_detect(htxt, pat_kw) | str_detect(htxt, pat_title) | is_title_text(htxt)))
  if (length(target_idx) == 0) {
    target_idx <- which(htag %in% heading_levels & str_detect(htxt, "(lead|exec|board|govern|senior|team)"))
    if (length(target_idx) == 0) return("")
  }
  
  md_prefix <- function(tag) {
    tag <- tolower(tag)
    if (tag == "h1") return("# ")
    if (tag == "h2") return("## ")
    if (tag == "h3") return("### ")
    if (tag == "h4") return("#### ")
    "## "
  }
  
  out_sections <- map_chr(target_idx, function(i) {
    start <- headings_all[[i]]
    start_tag <- xml_name(start)
    start_txt <- html_text2(start)
    current_level <- as.integer(str_sub(tolower(start_tag), 2, 2))
    
    # look backward for name in a previous heading sibling
    name_heading_txt <- NULL
    if (include_name_heading) {
      parent   <- xml_parent(start)
      siblings <- xml_children(parent)
      idx      <- which(vapply(siblings, identical, logical(1), y = start))
      if (length(idx) == 1 && idx > 1) {
        for (k in seq.int(idx - 1, 1)) {
          nm <- tolower(xml_name(siblings[[k]]))
          if (grepl("^h[1-4]$", nm)) {
            cand <- str_squish(html_text2(siblings[[k]]))
            if (person_like(cand) && !is_title_text(cand)) name_heading_txt <- cand
            break
          }
        }
      }
    }
    
    # span until next heading of same/higher level (optionally include h4)
    parent   <- xml_parent(start)
    siblings <- xml_children(parent)
    sib_idx  <- which(vapply(siblings, identical, logical(1), y = start))
    if (length(sib_idx) == 0) return("")
    
    stop_idx <- length(siblings) + 1
    if (sib_idx < length(siblings)) {
      for (j in (sib_idx + 1):length(siblings)) {
        nm <- tolower(xml_name(siblings[[j]]))
        if (grepl("^h[1-4]$", nm)) {
          lvl <- as.integer(str_sub(nm, 2, 2))
          if (lvl <= current_level || (!include_h4_in_span && lvl == 4)) { stop_idx <- j; break }
        }
      }
    }
    
    if (sib_idx + 1 >= stop_idx) {
      parts <- c(
        if (!is.null(name_heading_txt)) paste0("## ", name_heading_txt) else NULL,
        paste0(md_prefix(start_tag), str_squish(start_txt))
      )
      return(paste(parts, collapse = "\n"))
    }
    
    nodes <- siblings[(sib_idx + 1):(stop_idx - 1)]
    xml_find_all(nodes, ".//script|.//style|.//noscript|.//nav|.//footer|.//form|.//header") |> xml_remove()
    
    subheading_lines <- character(0)
    if (include_subheadings) {
      subs <- xml_find_all(nodes, ".//h2|.//h3|.//h4")
      if (length(subs) > 0) {
        subheading_lines <- vapply(subs, function(h) {
          paste0(md_prefix(xml_name(h)), str_squish(html_text2(h)))
        }, character(1))
      }
    }
    
    body_txt <- paste(vapply(nodes, html_text2, character(1)), collapse = "\n") |> normalize_text()
    
    synth_line <- NULL
    if (!is.null(name_heading_txt) && is_title_text(start_txt) && synth_name_title_line) {
      synth_line <- paste0(name_heading_txt, ", ", str_squish(start_txt))
    }
    
    parts <- c(
      if (!is.null(name_heading_txt)) paste0("## ", name_heading_txt) else NULL,
      if (include_headings) paste0(md_prefix(start_tag), str_squish(start_txt)) else NULL,
      synth_line,
      if (length(subheading_lines) > 0) paste(subheading_lines, collapse = "\n") else NULL,
      body_txt
    )
    section <- paste(parts, collapse = "\n") |> str_trim()
    
    if (nchar(section) < min_chars) "" else section
  })
  
  out_sections <- out_sections[nchar(out_sections) > 0]
  paste(unique(out_sections), collapse = "\n\n--- SECTION BREAK ---\n\n")
}

# ---------- Profile-aware fetcher (Part B) ----------
fetch_leadership_profiled <- function(
    url,
    profiles,
    out_dir = "data/raw",
    prefix  = "leaders"
) {
  dir_create(out_dir, recurse = TRUE)
  
  p <- profile_for_url(url, profiles)
  
  # Rebuild the TITLE regex for this URL (defaults + per-domain extras)
  TITLES_RUN <- c(TITLE_LEXICON, p$title_variants_extra)
  TITLE_REGEX <<- build_title_regex(TITLES_RUN)
  
  # Fetch HTML (plain). If you later add chromote, branch on p$use_chromote.
  req <- request(url) |>
    req_user_agent("profiled/1.0") |>
    req_headers(Accept = "text/html,*/*;q=0.8") |>
    req_timeout(30)
  resp <- try(req_perform(req), silent = TRUE)
  if (inherits(resp, "try-error") || resp_status(resp) >= 400) {
    msg <- if (inherits(resp, "try-error")) paste("fetch failed:", attr(resp, "condition")$message)
    else paste("HTTP", resp_status(resp))
    return(list(ok = FALSE, message = msg, text = "", file = NA_character_, profile = p))
  }
  
  doc <- try(read_html(resp_body_string(resp)), silent = TRUE)
  if (inherits(doc, "try-error")) {
    return(list(ok = FALSE, message = "parse failed", text = "", file = NA_character_, profile = p))
  }
  
  section_txt <- harvest_sections_by_headings(
    doc,
    heading_levels        = p$heading_levels,
    keywords              = p$keywords,
    title_variants        = TITLES_RUN,
    include_headings      = p$include_headings,
    include_subheadings   = p$include_subheadings,
    include_h4_in_span    = p$include_h4_in_span,
    include_name_heading  = p$include_name_heading,
    synth_name_title_line = p$synth_name_title_line,
    min_chars             = p$min_chars
  )
  
  if (!nzchar(section_txt) || nchar(section_txt) < p$min_chars) {
    if (isTRUE(p$use_css_fallback)) {
      fb <- css_card_fallback(doc, min_cards = p$css_min_cards %||% 3)
      if (nzchar(fb) && nchar(fb) > nchar(section_txt)) section_txt <- fb
    }
  }
  
  if (!nzchar(section_txt)) {
    return(list(ok = FALSE, message = "no text after harvest/fallback", text = "", file = NA_character_, profile = p))
  }
  
  host <- url_parse(url)$domain %||% "site"
  path_bits <- url_parse(url)$path %||% ""
  stem <- paste0(safe(host), "_", safe(path_bits)); if (!nzchar(stem)) stem <- safe(host)
  stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
  fname <- glue("{prefix}_{stem}_{stamp}.txt") |> safe()
  fpath <- file.path(out_dir, fname)
  writeLines(section_txt, fpath, useBytes = TRUE)
  
  list(ok = TRUE, message = "ok", text = section_txt, file = fpath, profile = p)
}

# ---------- Patterns that boost confidence ----------
PATTERN_NAME_COMMA_TITLE <- function(name, title_regex) paste0("(?i)\\b", str_replace_all(name,"\\.","\\."), "\\b\\s*,\\s*(?:the\\s+)?", title_regex)
PATTERN_TITLE_NAME       <- function(name, title_regex) paste0("(?i)", title_regex, "\\s+\\b", str_replace_all(name,"\\.","\\."), "\\b")
PATTERN_NAME_IS_TITLE    <- function(name, title_regex) paste0("(?i)\\b", str_replace_all(name,"\\.","\\."), "\\b\\s+is\\s+(?:the\\s+|a\\s+)?", title_regex)

# ---------- NLP: extract ALL candidates ----------
extract_name_title_org <- function(text,
                                   min_confidence = 2L,
                                   window_sentences = 1L,
                                   model = .SPACY_MODEL,
                                   condaenv = .SPACY_CONDAENV) {
  stopifnot(is.character(text), length(text) == 1)
  init_spacy_safe(model, condaenv)
  
  sents <- tokenizers::tokenize_sentences(text)[[1]]
  if (length(sents) == 0) {
    return(tibble(NAME=character(), TITLE=character(), ORG=character(), CONFIDENCE=integer(), SENTENCE=character()))
  }
  
  parsed <- spacyr::spacy_parse(text, entity = TRUE, nounphrase = FALSE)
  ents   <- spacyr::entity_extract(parsed, type = "named")
  if (nrow(ents) == 0) {
    return(tibble(NAME=character(), TITLE=character(), ORG=character(), CONFIDENCE=integer(), SENTENCE=character()))
  }
  if ("sentence_id" %in% names(ents)) ents <- dplyr::rename(ents, sent_id = sentence_id)
  
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
      tidyr::expand_grid(TITLE = tt, NAME = pp, ORG = oo) |> dplyr::mutate(SENTENCE = sent)
    })) |>
    dplyr::select(sent_id, combos) |>
    tidyr::unnest(combos)
  
  pull_nearby <- function(idx, need_col) {
    rng  <- max(1, idx - window_sentences):min(length(sents), idx + window_sentences)
    pool <- merged |> dplyr::filter(sent_id %in% rng)
    vec  <- unique(unlist(pool[[need_col]]))
    if (length(vec) == 0) NA_character_ else vec
  }
  
  cands_aug <- cand_in_sent |>
    dplyr::rowwise() |>
    dplyr::mutate(
      TITLE = if (is.na(TITLE) || TITLE == "") { v <- pull_nearby(sent_id, "TITLE"); if (length(v) > 1) v[1] else v } else TITLE,
      ORG   = if (is.na(ORG)   || ORG == "")   { v <- pull_nearby(sent_id, "ORG");   if (length(v) > 1) v[1] else v } else ORG
    ) |>
    dplyr::ungroup()
  
  score_row <- function(name, title, org, sentence) {
    score <- 0L
    if (!is.na(name)  && nzchar(name))  score <- score + 1L
    if (!is.na(title) && nzchar(title)) score <- score + 1L
    if (!is.na(org)   && nzchar(org))   score <- score + 1L
    if (!is.na(name) && nzchar(name) && !is.na(title) && nzchar(title)) {
      t_re <- paste0("(?i)\\b", str_replace_all(title,"\\.","\\."), "\\b")
      if (str_detect(sentence, PATTERN_NAME_COMMA_TITLE(name, t_re))) score <- score + 2L
      if (str_detect(sentence, PATTERN_TITLE_NAME(name, t_re)))       score <- score + 2L
      if (str_detect(sentence, PATTERN_NAME_IS_TITLE(name, t_re)))    score <- score + 2L
    }
    if (!is.na(org) && nzchar(org) && !is.na(title) && nzchar(title)) {
      t_re <- paste0("(?i)\\b", str_replace_all(title,"\\.","\\."), "\\b")
      o_re <- paste0("(?i)\\b", str_replace_all(org,  "\\.","\\."), "\\b")
      if (str_detect(sentence, paste0(t_re, "\\s+(?:of|at)\\s+", o_re))) score <- score + 1L
    }
    score
  }
  
  cands_aug |>
    dplyr::mutate(CONFIDENCE = purrr::pmap_int(list(NAME, TITLE, ORG, SENTENCE), score_row)) |>
    dplyr::arrange(dplyr::desc(CONFIDENCE), dplyr::desc(!is.na(NAME)), dplyr::desc(!is.na(TITLE))) |>
    dplyr::distinct(NAME, TITLE, ORG, SENTENCE, .keep_all = TRUE) |>
    dplyr::filter(CONFIDENCE >= min_confidence) |>
    dplyr::transmute(
      NAME = ifelse(is.na(NAME), "", NAME),
      TITLE = ifelse(is.na(TITLE), "", str_squish(TITLE)),
      ORG = ifelse(is.na(ORG), "", ORG),
      CONFIDENCE,
      SENTENCE = str_squish(SENTENCE)
    )
}
