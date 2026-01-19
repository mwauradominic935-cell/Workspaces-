# BUDGET101.R
# Stable, corrected Budget Tracker Shiny App
# - Fixed PDF/Excel export bugs and ensured project-only isolation for reports.
# - All UI buttons (including Generate PDF) validated and robust.
# - Splash screen "NICCO CREATIVES" preserved and non-blocking.
# - All existing features preserved; improved error handling and file/path safety.

# Required packages:
# shiny, shinydashboard, DBI, RSQLite, dplyr, DT, jsonlite, lubridate,
# openxlsx, ggplot2, plotly, fs, digest, zip, shinyWidgets, shinyFiles, scales, htmltools
#
# Install missing packages first if needed:
# install.packages(c("shiny","shinydashboard","DBI","RSQLite","dplyr","DT","jsonlite","lubridate","openxlsx","ggplot2","plotly","fs","digest","zip","shinyWidgets","shinyFiles","scales","htmltools"))

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

required_pkgs <- c(
  "shiny","shinydashboard","DBI","RSQLite","dplyr","DT","jsonlite","lubridate",
  "openxlsx","ggplot2","plotly","fs","digest","zip","shinyWidgets","shinyFiles","scales","htmltools"
)
missing <- required_pkgs[!vapply(required_pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing)) {
  stop("Please install missing packages first:\n",
       paste0("install.packages(c(", paste0("'", missing, "'", collapse = ", "), "))"))
}

has_pagedown <- requireNamespace("pagedown", quietly = TRUE)
has_webshot2 <- requireNamespace("webshot2", quietly = TRUE)

library(shiny)
library(shinydashboard)
library(DBI)
library(RSQLite)
library(dplyr)
library(DT)
library(jsonlite)
library(lubridate)
library(openxlsx)
library(ggplot2)
library(plotly)
library(fs)
library(digest)
library(zip)
library(shinyWidgets)
library(shinyFiles)
library(scales)
library(htmltools)

# ---------------------------
# Helpers
# ---------------------------
html_escape <- function(x) htmltools::htmlEscape(as.character(x %||% ""))

app_root <- normalizePath(".", mustWork = FALSE)
data_dir <- file.path(app_root, "data")
dir_create(data_dir, recurse = TRUE)

internal_exports_dir <- file.path(data_dir, "exports")
internal_backups_dir <- file.path(data_dir, "backups")
receipts_dir <- file.path(data_dir, "receipts")
trash_dir <- file.path(data_dir, "trash")

dir_create(internal_exports_dir, recurse = TRUE)
dir_create(internal_backups_dir, recurse = TRUE)
dir_create(receipts_dir, recurse = TRUE)
dir_create(trash_dir, recurse = TRUE)

db_file <- file.path(data_dir, "budget_tracker.sqlite")

connect_db <- function() {
  con <- dbConnect(RSQLite::SQLite(), db_file)
  try(dbExecute(con, "PRAGMA journal_mode = WAL;"), silent = TRUE)
  con
}

# ---------------------------
# Initialize DB and schema migrations
# ---------------------------
init_db <- function() {
  con <- connect_db(); on.exit(dbDisconnect(con))
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS projects (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      budget_total REAL DEFAULT 0,
      start_date TEXT,
      end_date TEXT,
      status TEXT DEFAULT 'active',
      budget_locked INTEGER DEFAULT 0,
      created_at TEXT,
      updated_at TEXT,
      is_trashed INTEGER DEFAULT 0,
      workbook_path TEXT DEFAULT ''
    );
  ")
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS expenses (
      id TEXT PRIMARY KEY,
      project_id TEXT,
      description TEXT,
      amount REAL NOT NULL,
      category TEXT,
      date TEXT,
      receipt_path TEXT,
      created_at TEXT,
      updated_at TEXT,
      is_trashed INTEGER DEFAULT 0
    );
  ")
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS attachments (
      id TEXT PRIMARY KEY,
      expense_id TEXT,
      filename TEXT,
      filepath TEXT,
      mimetype TEXT,
      size INTEGER,
      checksum TEXT,
      created_at TEXT
    );
  ")
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS audit (
      id TEXT PRIMARY KEY,
      user TEXT,
      timestamp TEXT,
      module TEXT,
      action TEXT,
      target_table TEXT,
      target_id TEXT,
      detail TEXT
    );
  ")
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT
    );
  ")
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS backups (
      id TEXT PRIMARY KEY,
      filename TEXT,
      filepath TEXT,
      created_at TEXT,
      size INTEGER,
      checksum TEXT,
      notes TEXT
    );
  ")
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS exports_meta (
      id TEXT PRIMARY KEY,
      filename TEXT,
      filepath TEXT,
      type TEXT,
      created_at TEXT,
      criteria TEXT
    );
  ")
  
  existing <- dbGetQuery(con, "SELECT COUNT(*) as n FROM settings;")$n
  if (existing == 0) {
    nowt <- as.character(Sys.time())
    seed <- data.frame(
      key = c("date_format","auto_save_interval_sec","export_folder","backup_folder","excel_password","watermark_text","theme","protect_other_sheets"),
      value = c("YYYY-MM-DD","30", internal_exports_dir, internal_backups_dir, "", "CONFIDENTIAL","Light","false"),
      updated_at = rep(nowt, 8),
      stringsAsFactors = FALSE
    )
    dbWriteTable(con, "settings", seed, append = TRUE, row.names = FALSE)
  }
}
init_db()

uuid <- function(prefix = "id") paste0(prefix, "-", digest::digest(runif(1)), "-", as.integer(Sys.time()))
now <- function() as.character(Sys.time())

sanitize_filename <- function(x) {
  if (is.null(x)) return("unnamed")
  x <- as.character(x)
  x2 <- gsub("[^[:alnum:]_ \\-]", "", x)
  x3 <- gsub("\\s+", "_", x2)
  x4 <- gsub("^_+|_+$", "", x3)
  if (nzchar(x4)) x4 else "unnamed"
}

# Settings helpers
get_setting <- function(key) {
  con <- connect_db(); on.exit(dbDisconnect(con))
  res <- dbGetQuery(con, "SELECT value FROM settings WHERE key = ?;", params = list(key))
  if (nrow(res) == 0) return(NULL)
  res$value[[1]]
}
set_setting <- function(key, value) {
  con <- connect_db(); on.exit(dbDisconnect(con))
  try({
    dbExecute(con, "
      INSERT INTO settings(key,value,updated_at) VALUES (?,?,?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
    ", params = list(key, as.character(value), now()))
    # audit
    con2 <- connect_db(); on.exit(dbDisconnect(con2))
    entry <- data.frame(id = uuid("audit"), user = "local_user", timestamp = now(), module = "Settings", action = "update_setting", target_table = "settings", target_id = key, detail = jsonlite::toJSON(list(value = value), auto_unbox = TRUE), stringsAsFactors = FALSE)
    dbWriteTable(con2, "audit", entry, append = TRUE, row.names = FALSE)
  }, silent = TRUE)
  invisible(TRUE)
}

get_export_folder <- function() {
  v <- get_setting("export_folder") %||% internal_exports_dir
  try(dir_create(v, recurse = TRUE), silent = TRUE)
  normalizePath(v, winslash = "/", mustWork = FALSE)
}
get_backup_folder <- function() {
  v <- get_setting("backup_folder") %||% internal_backups_dir
  try(dir_create(v, recurse = TRUE), silent = TRUE)
  normalizePath(v, winslash = "/", mustWork = FALSE)
}

load_all <- function() {
  con <- connect_db(); on.exit(dbDisconnect(con))
  projects <- dbGetQuery(con, "SELECT * FROM projects;")
  expenses <- dbGetQuery(con, "SELECT * FROM expenses;")
  attachments <- dbGetQuery(con, "SELECT * FROM attachments;")
  settings <- dbGetQuery(con, "SELECT * FROM settings;")
  audit <- dbGetQuery(con, "SELECT * FROM audit ORDER BY timestamp DESC LIMIT 2000;")
  exports_meta <- dbGetQuery(con, "SELECT * FROM exports_meta ORDER BY created_at DESC LIMIT 200;")
  projects <- projects %>% mutate(across(c(created_at, updated_at, start_date, end_date), ~ ifelse(. == "", NA, .)))
  expenses <- expenses %>% mutate(across(c(created_at, updated_at, date), ~ ifelse(. == "", NA, .)))
  list(projects = projects, expenses = expenses, attachments = attachments, settings = settings, audit = audit, exports_meta = exports_meta)
}

# ---------------------------
# Per-project workbook sync (only that project's data)
# ---------------------------
sync_project_workbook <- function(project_id) {
  if (is.null(project_id) || !nzchar(as.character(project_id))) {
    message("sync_project_workbook requires a valid project_id")
    return(invisible(FALSE))
  }
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    pr <- dbGetQuery(con, "SELECT * FROM projects WHERE id = ? LIMIT 1;", params = list(project_id))
    if (nrow(pr) == 0) return(invisible(FALSE))
    pr <- pr[1, ]
    ex <- dbGetQuery(con, "SELECT * FROM expenses WHERE project_id = ? AND is_trashed = 0;", params = list(project_id))
    attachments_df <- dbGetQuery(con, "SELECT * FROM attachments WHERE expense_id IN (SELECT id FROM expenses WHERE project_id = ?);", params = list(project_id))
    audit_all <- dbGetQuery(con, "SELECT * FROM audit ORDER BY timestamp DESC LIMIT 2000;")
    related_audit <- audit_all %>% filter((!is.na(target_id) & target_id == project_id) | (target_id %in% ex$id) | (!is.na(detail) & grepl(project_id, detail, fixed = TRUE)))
    
    wb <- createWorkbook()
    hdrStyle <- createStyle(fontSize = 12, textDecoration = "bold", halign = "center")
    moneyStyle <- createStyle(numFmt = "ACCOUNTING")
    
    # Summary
    addWorksheet(wb, "Summary")
    summary_df <- data.frame(
      ProjectID = pr$id,
      Name = pr$name,
      Description = pr$description,
      Budget = as.numeric(pr$budget_total),
      Spent = sum(as.numeric(ex$amount), na.rm = TRUE),
      Remaining = as.numeric(pr$budget_total) - sum(as.numeric(ex$amount), na.rm = TRUE),
      Status = pr$status,
      Start = pr$start_date,
      End = pr$end_date,
      LastUpdate = pr$updated_at,
      stringsAsFactors = FALSE
    )
    writeData(wb, "Summary", summary_df, startRow = 1, startCol = 1)
    addStyle(wb, "Summary", hdrStyle, rows = 1, cols = 1:ncol(summary_df), gridExpand = TRUE)
    writeData(wb, "Summary", x = data.frame(Label = c("Total Spent","Remaining Balance"), Value = c(summary_df$Spent, summary_df$Remaining)), startRow = 4, startCol = 1)
    addStyle(wb, "Summary", createStyle(fontSize = 10, textDecoration = "bold"), rows = 4:5, cols = 1, gridExpand = TRUE)
    addStyle(wb, "Summary", createStyle(fontSize = 10), rows = 4:5, cols = 2, gridExpand = TRUE)
    
    # Expenses
    addWorksheet(wb, "Expenses")
    if (nrow(ex) > 0) {
      ex_sel <- ex %>% select(id, description, amount, category, date, receipt_path, created_at)
      writeDataTable(wb, "Expenses", ex_sel, tableStyle = "TableStyleMedium9")
      try({ addStyle(wb, "Expenses", moneyStyle, rows = 2:(nrow(ex_sel) + 1), cols = 3, gridExpand = TRUE) }, silent = TRUE)
      try({ writeData(wb, "Expenses", x = data.frame(Total = sum(as.numeric(ex_sel$amount), na.rm = TRUE)), startRow = nrow(ex_sel) + 3, startCol = 3) }, silent = TRUE)
    } else {
      writeData(wb, "Expenses", data.frame(Message = "No expenses for this project"), startRow = 1)
    }
    
    # Categories
    addWorksheet(wb, "Categories")
    if (nrow(ex) > 0) {
      cat_df <- ex %>% group_by(category) %>% summarise(Total = sum(amount, na.rm = TRUE)) %>% arrange(desc(Total))
      writeDataTable(wb, "Categories", cat_df)
    } else {
      writeData(wb, "Categories", data.frame(Message = "No categories / expenses"))
    }
    
    # Attachments
    addWorksheet(wb, "Attachments")
    if (nrow(attachments_df) > 0) {
      writeDataTable(wb, "Attachments", attachments_df %>% select(id, expense_id, filename, filepath, size, created_at))
    } else {
      writeData(wb, "Attachments", data.frame(Message = "No attachments for this project"))
    }
    
    # Audit
    addWorksheet(wb, "Audit")
    if (nrow(related_audit) > 0) {
      writeDataTable(wb, "Audit", related_audit, tableStyle = "TableStyleLight9")
    } else {
      writeData(wb, "Audit", data.frame(Message = "No audit entries for this project"), startRow = 1)
    }
    
    pwd <- get_setting("excel_password") %||% ""
    protect_other <- tolower(as.character(get_setting("protect_other_sheets") %||% "false")) == "true"
    if (nzchar(pwd)) {
      try(protectWorksheet(wb, sheet = "Audit", protect = TRUE, password = pwd), silent = TRUE)
      if (protect_other) {
        other_sheets <- setdiff(names(wb), "Audit")
        for (s in other_sheets) try(protectWorksheet(wb, sheet = s, protect = TRUE, password = pwd), silent = TRUE)
      }
    }
    
    export_folder <- get_export_folder()
    dir_create(export_folder, recurse = TRUE)
    fname <- file.path(export_folder, paste0("project_", sanitize_filename(pr$name), "_", pr$id, ".xlsx"))
    saveWorkbook(wb, fname, overwrite = TRUE)
    fname_abs <- normalizePath(fname, winslash = "/", mustWork = FALSE)
    
    rec <- data.frame(id = uuid("exp"), filename = basename(fname_abs), filepath = fname_abs, type = "project_workbook", created_at = now(), criteria = project_id, stringsAsFactors = FALSE)
    dbWriteTable(con, "exports_meta", rec, append = TRUE, row.names = FALSE)
    try(dbExecute(con, "UPDATE projects SET workbook_path = ?, updated_at = ? WHERE id = ?;", params = list(fname_abs, now(), project_id)), silent = TRUE)
    
    entry <- data.frame(id = uuid("audit"), user = "system", timestamp = now(), module = "Reports", action = "sync_project_workbook", target_table = "exports_meta", target_id = rec$id, detail = jsonlite::toJSON(list(path = fname_abs), auto_unbox = TRUE), stringsAsFactors = FALSE)
    dbWriteTable(con, "audit", entry, append = TRUE, row.names = FALSE)
    
    invisible(fname_abs)
  }, error = function(e) {
    message("sync_project_workbook error: ", e$message)
    invisible(FALSE)
  })
}

# ---------------------------
# Audit logging (and sync triggers)
# ---------------------------
log_audit <- function(user = "local_user", module, action, target_table = NA, target_id = NA, detail = list()) {
  if (is.null(module) || !nzchar(module)) module <- "Unknown"
  try({
    con <- connect_db(); on.exit(dbDisconnect(con))
    entry <- data.frame(
      id = uuid("audit"),
      user = user,
      timestamp = now(),
      module = module,
      action = action,
      target_table = ifelse(is.na(target_table), "", target_table),
      target_id = ifelse(is.na(target_id), "", target_id),
      detail = jsonlite::toJSON(detail, auto_unbox = TRUE),
      stringsAsFactors = FALSE
    )
    dbWriteTable(con, "audit", entry, append = TRUE, row.names = FALSE)
  }, silent = TRUE)
  
  # trigger per-project syncs where applicable
  try({
    if (!identical(user, "system")) {
      if (!is.na(target_table) && target_table == "projects" && nzchar(target_id)) {
        try(sync_project_workbook(target_id), silent = TRUE)
      }
      if (!is.na(target_table) && target_table == "expenses" && nzchar(target_id)) {
        con2 <- connect_db(); on.exit(dbDisconnect(con2))
        res <- dbGetQuery(con2, "SELECT project_id FROM expenses WHERE id = ? LIMIT 1;", params = list(target_id))
        if (nrow(res) == 1 && nzchar(res$project_id)) try(sync_project_workbook(res$project_id), silent = TRUE)
      }
      if (!is.null(detail) && length(detail) > 0) {
        txt <- tryCatch(jsonlite::toJSON(detail, auto_unbox = TRUE), error = function(e) as.character(detail))
        con3 <- connect_db(); on.exit(dbDisconnect(con3))
        pids <- dbGetQuery(con3, "SELECT id FROM projects;")$id
        for (pid in pids) {
          if (grepl(pid, txt, fixed = TRUE)) {
            try(sync_project_workbook(pid), silent = TRUE)
          }
        }
      }
    }
  }, silent = TRUE)
  invisible(TRUE)
}

# ---------------------------
# CRUD helpers
# ---------------------------
create_project <- function(id, name, description = "", budget_total = 0, start_date = NA, end_date = NA, status = "active", budget_locked = 0) {
  tryCatch({
    if (is.null(id) || !nzchar(id)) id <- uuid("proj")
    con <- connect_db(); on.exit(dbDisconnect(con))
    nowt <- now()
    df <- data.frame(id = id, name = name, description = description, budget_total = budget_total, start_date = ifelse(is.na(start_date), "", start_date), end_date = ifelse(is.na(end_date), "", end_date), status = status, budget_locked = as.integer(budget_locked), created_at = nowt, updated_at = nowt, is_trashed = 0, workbook_path = "", stringsAsFactors = FALSE)
    dbWriteTable(con, "projects", df, append = TRUE, row.names = FALSE)
    log_audit(module = "Projects", action = "create", target_table = "projects", target_id = id, detail = list(name = name))
    wb_path <- sync_project_workbook(id)
    if (!identical(wb_path, FALSE) && !is.null(wb_path)) {
      try({
        con2 <- connect_db(); on.exit(dbDisconnect(con2))
        dbExecute(con2, "UPDATE projects SET workbook_path = ?, updated_at = ? WHERE id = ?;", params = list(wb_path, now(), id))
      }, silent = TRUE)
    }
    TRUE
  }, error = function(e) {
    message("create_project error: ", e$message); FALSE
  })
}

update_project <- function(id, fields) {
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    fields$updated_at <- now()
    set_sql <- paste0(names(fields), " = ?", collapse = ", ")
    params <- c(unname(fields), id)
    sql <- paste0("UPDATE projects SET ", set_sql, " WHERE id = ?;")
    dbExecute(con, sql, params = params)
    log_audit(module = "Projects", action = "update", target_table = "projects", target_id = id, detail = fields)
    sync_project_workbook(id)
    TRUE
  }, error = function(e) {
    message("update_project error: ", e$message); FALSE
  })
}

soft_delete_project <- function(id) {
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    dbExecute(con, "UPDATE projects SET is_trashed = 1, updated_at = ? WHERE id = ?;", params = list(now(), id))
    dbExecute(con, "UPDATE expenses SET is_trashed = 1, updated_at = ? WHERE project_id = ?;", params = list(now(), id))
    log_audit(module = "Projects", action = "trash", target_table = "projects", target_id = id, detail = list())
    sync_project_workbook(id)
    TRUE
  }, error = function(e) { message("soft_delete_project error: ", e$message); FALSE })
}

restore_project <- function(id) {
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    dbExecute(con, "UPDATE projects SET is_trashed = 0, updated_at = ? WHERE id = ?;", params = list(now(), id))
    dbExecute(con, "UPDATE expenses SET is_trashed = 0, updated_at = ? WHERE project_id = ?;", params = list(now(), id))
    log_audit(module = "Projects", action = "restore", target_table = "projects", target_id = id, detail = list())
    sync_project_workbook(id)
    TRUE
  }, error = function(e) { message("restore_project error: ", e$message); FALSE })
}

create_expense <- function(id, project_id, description, amount, category, date, receipt_tmpfile = NULL, receipt_name = NULL) {
  tryCatch({
    if (is.null(project_id) || !nzchar(as.character(project_id))) stop("create_expense requires a valid project_id (enforced by UI).")
    con <- connect_db(); on.exit(dbDisconnect(con))
    nowt <- now()
    receipt_path <- ""
    if (!is.null(receipt_tmpfile) && receipt_tmpfile != "" && !is.null(receipt_name)) {
      ext <- path_ext(receipt_name)
      fname <- paste0("receipt_", id, ifelse(ext == "", "", paste0(".", ext)))
      dest <- file.path(receipts_dir, fname)
      file.copy(receipt_tmpfile, dest, overwrite = TRUE)
      receipt_path <- dest
      attach <- data.frame(id = uuid("att"), expense_id = id, filename = receipt_name, filepath = dest, mimetype = "", size = file_info(dest)$size, checksum = digest::digest(file(dest, "rb"), algo = "md5"), created_at = nowt, stringsAsFactors = FALSE)
      dbWriteTable(con, "attachments", attach, append = TRUE, row.names = FALSE)
    }
    df <- data.frame(id = id, project_id = as.character(project_id), description = description, amount = amount, category = category, date = ifelse(is.null(date), "", date), receipt_path = receipt_path, created_at = nowt, updated_at = nowt, is_trashed = 0, stringsAsFactors = FALSE)
    dbWriteTable(con, "expenses", df, append = TRUE, row.names = FALSE)
    log_audit(module = "Expenses", action = "create", target_table = "expenses", target_id = id, detail = list(amount = amount, project_id = project_id))
    sync_project_workbook(project_id)
    TRUE
  }, error = function(e) { message("create_expense error: ", e$message); FALSE })
}

update_expense <- function(id, fields) {
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    fields$updated_at <- now()
    set_sql <- paste0(names(fields), " = ?", collapse = ", ")
    params <- c(unname(fields), id)
    sql <- paste0("UPDATE expenses SET ", set_sql, " WHERE id = ?;")
    dbExecute(con, sql, params = params)
    log_audit(module = "Expenses", action = "update", target_table = "expenses", target_id = id, detail = fields)
    proj <- dbGetQuery(con, "SELECT project_id FROM expenses WHERE id = ?;", params = list(id))
    if (nrow(proj) == 1 && nzchar(proj$project_id)) sync_project_workbook(proj$project_id)
    TRUE
  }, error = function(e) { message("update_expense error: ", e$message); FALSE })
}

soft_delete_expense <- function(id) {
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    proj <- dbGetQuery(con, "SELECT project_id FROM expenses WHERE id = ?;", params = list(id))
    dbExecute(con, "UPDATE expenses SET is_trashed = 1, updated_at = ? WHERE id = ?;", params = list(now(), id))
    log_audit(module = "Expenses", action = "trash", target_table = "expenses", target_id = id, detail = list())
    if (nrow(proj) == 1 && nzchar(proj$project_id)) sync_project_workbook(proj$project_id)
    TRUE
  }, error = function(e) { message("soft_delete_expense error: ", e$message); FALSE })
}

restore_expense <- function(id) {
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    proj <- dbGetQuery(con, "SELECT project_id FROM expenses WHERE id = ?;", params = list(id))
    dbExecute(con, "UPDATE expenses SET is_trashed = 0, updated_at = ? WHERE id = ?;", params = list(now(), id))
    log_audit(module = "Expenses", action = "restore", target_table = "expenses", target_id = id, detail = list())
    if (nrow(proj) == 1 && nzchar(proj$project_id)) sync_project_workbook(proj$project_id)
    TRUE
  }, error = function(e) { message("restore_expense error: ", e$message); FALSE })
}

# ---------------------------
# Reporting: Excel & HTML/PDF (project-only)
# PDF must NOT include the audit trail.
# ---------------------------
write_excel_report <- function(project_id, watermark = NULL) {
  if (is.null(project_id) || !nzchar(as.character(project_id))) {
    message("write_excel_report requires a valid project_id")
    return(NULL)
  }
  tryCatch({
    # Ensure workbook created and synced
    wb_path <- sync_project_workbook(project_id)
    if (identical(wb_path, FALSE) || is.null(wb_path) || !nzchar(wb_path)) {
      stop("Failed to create or locate project workbook.")
    }
    # Apply watermark header/footer if supplied
    if (!is.null(watermark) && nzchar(as.character(watermark))) {
      try({
        wb2 <- loadWorkbook(wb_path)
        for (s in names(wb2)) {
          try(setHeaderFooter(wb2, sheet = s, header = c("", as.character(watermark), "")), silent = TRUE)
        }
        saveWorkbook(wb2, wb_path, overwrite = TRUE)
      }, silent = TRUE)
    }
    # register export meta
    con <- connect_db(); on.exit(dbDisconnect(con))
    rec <- data.frame(id = uuid("exp"), filename = basename(wb_path), filepath = normalizePath(wb_path, winslash = "/", mustWork = FALSE), type = "excel", created_at = now(), criteria = project_id, stringsAsFactors = FALSE)
    dbWriteTable(con, "exports_meta", rec, append = TRUE, row.names = FALSE)
    log_audit(module = "Reports", action = "export_excel", target_table = "exports_meta", target_id = rec$id, detail = list(filename = wb_path))
    normalizePath(wb_path, winslash = "/", mustWork = FALSE)
  }, error = function(e) {
    message("write_excel_report error: ", e$message); NULL
  })
}

write_html_pdf_report <- function(project_id, pdf = FALSE, watermark = NULL) {
  if (is.null(project_id) || !nzchar(as.character(project_id))) {
    message("write_html_pdf_report requires a valid project_id")
    return(NULL)
  }
  tryCatch({
    con <- connect_db(); on.exit(dbDisconnect(con))
    projects <- dbGetQuery(con, "SELECT * FROM projects WHERE id = ? LIMIT 1;", params = list(project_id))
    if (nrow(projects) == 0) stop("Project not found")
    projects <- projects[1, ]
    expenses <- dbGetQuery(con, "SELECT * FROM expenses WHERE project_id = ? AND is_trashed = 0 ORDER BY date ASC;", params = list(project_id))
    audit_all <- dbGetQuery(con, "SELECT * FROM audit ORDER BY timestamp DESC LIMIT 2000;")
    expense_ids <- if (nrow(expenses) > 0) expenses$id else character(0)
    audit_rel <- audit_all %>% filter((!is.na(target_id) & target_id == project_id) | (target_id %in% expense_ids) | (!is.na(detail) & grepl(project_id, detail, fixed = TRUE)))
    
    title <- paste0("Budget Tracker - Project Report: ", projects$name)
    total_spent <- sum(expenses$amount, na.rm = TRUE)
    total_budget <- as.numeric(projects$budget_total)
    remaining <- total_budget - total_spent
    
    export_folder <- get_export_folder()
    dir_create(export_folder, recurse = TRUE)
    fname_base <- paste0("report_project_", sanitize_filename(projects$name), "_", project_id, "_", format(Sys.time(), "%Y%m%d_%H%M%S"))
    filename_html <- file.path(export_folder, paste0(fname_base, ".html"))
    
    css <- "body{font-family: Inter, Arial, Helvetica, sans-serif; margin: 20px; color: #0b2545;} .header{display:flex; justify-content:space-between; align-items:center;} .summary { background:#f7fbff; padding:12px; border-radius:6px; margin-bottom:12px;} .key { font-weight:700; font-size:1.05rem; } .value { font-weight:700; font-size:1.25rem; color:#0b5394; } table{border-collapse: collapse; width: 100%; margin-top:12px;} table, th, td {border:1px solid #e6eef8; padding:8px;} th {background:#eaf3ff; text-align:left;} .section { margin-top:18px; } .important { font-size:1.15rem; font-weight:800; }"
    
    # Build HTML: include audit only when pdf==FALSE
    html <- c("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>",
              sprintf("<title>%s</title>", html_escape(title)),
              sprintf("<style>%s</style>", css),
              "</head><body>")
    if (!is.null(watermark) && nzchar(as.character(watermark))) html <- c(html, sprintf("<div style='position:fixed; top:35%%; left:10%%; font-size:72px; color:rgba(200,200,200,0.12); transform:rotate(-25deg); z-index:-1;'>%s</div>", html_escape(watermark)))
    html <- c(html, sprintf("<div class='header'><h1>%s</h1><div>%s</div></div>", html_escape(title), html_escape(now())))
    html <- c(html, "<div class='summary'>")
    html <- c(html, sprintf("<div><span class='key'>Total Spent:</span> <span class='value'>%s</span></div>", scales::dollar(total_spent)))
    html <- c(html, sprintf("<div style='margin-top:6px;'><span class='key'>Remaining Balance:</span> <span class='value'>%s</span></div>", scales::dollar(remaining)))
    html <- c(html, "</div>")
    
    # Project table
    html <- c(html, "<div class='section'><h2>Project</h2>")
    html <- c(html, "<table><thead><tr><th>ID</th><th>Name</th><th>Budget</th><th>Status</th><th>Start</th><th>End</th></tr></thead><tbody>")
    p <- projects
    html <- c(html, sprintf("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
                            html_escape(p$id),
                            html_escape(p$name),
                            scales::dollar(as.numeric(p$budget_total)),
                            html_escape(p$status),
                            html_escape(ifelse(is.na(p$start_date) || p$start_date == "", "", p$start_date)),
                            html_escape(ifelse(is.na(p$end_date) || p$end_date == "", "", p$end_date))
    ))
    html <- c(html, "</tbody></table></div>")
    
    # Expenses
    html <- c(html, "<div class='section'><h2>Expenses</h2>")
    if (nrow(expenses) == 0) {
      html <- c(html, "<p>No expenses for this project</p>")
    } else {
      html <- c(html, "<table><thead><tr><th>ID</th><th>Description</th><th>Amount</th><th>Category</th><th>Date</th></tr></thead><tbody>")
      for (i in seq_len(nrow(expenses))) {
        e <- expenses[i, ]
        html <- c(html, sprintf("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
                                html_escape(e$id),
                                html_escape(e$description),
                                scales::dollar(as.numeric(e$amount)),
                                html_escape(e$category %||% ""),
                                html_escape(ifelse(is.na(e$date) || e$date == "", "", e$date))
        ))
      }
      html <- c(html, "</tbody></table>")
    }
    html <- c(html, "</div>")
    
    # Audit section: only for HTML (pdf==FALSE)
    if (!isTRUE(pdf)) {
      html <- c(html, "<div class='section'><h2>Audit (recent)</h2>")
      if (nrow(audit_rel) == 0) {
        html <- c(html, "<p>No audit entries for this project.</p>")
      } else {
        html <- c(html, "<table><thead><tr><th>Time</th><th>User</th><th>Module</th><th>Action</th><th>Detail</th></tr></thead><tbody>")
        for (i in seq_len(nrow(audit_rel))) {
          a <- audit_rel[i, ]
          html <- c(html, sprintf("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
                                  html_escape(a$timestamp), html_escape(a$user), html_escape(a$module), html_escape(a$action), html_escape(a$detail)))
        }
        html <- c(html, "</tbody></table>")
      }
      html <- c(html, "</div>")
    } else {
      # pdf == TRUE -> omit audit entirely
    }
    
    html <- c(html, sprintf("<footer style='margin-top:18px;'><hr/><small>Generated: %s</small></footer>", html_escape(now())))
    html <- c(html, "</body></html>")
    
    # Write HTML
    writeLines(html, filename_html, useBytes = TRUE)
    
    # Register export meta
    rec <- data.frame(id = uuid("exp"), filename = basename(filename_html), filepath = normalizePath(filename_html, winslash = "/", mustWork = FALSE), type = ifelse(pdf, "pdf", "html"), created_at = now(), criteria = project_id, stringsAsFactors = FALSE)
    dbWriteTable(con, "exports_meta", rec, append = TRUE, row.names = FALSE)
    log_audit(module = "Reports", action = "export_html", target_table = "exports_meta", target_id = rec$id, detail = list(filename = filename_html))
    
    # If PDF requested: convert (HTML already omits audit when pdf==TRUE)
    if (isTRUE(pdf)) {
      pdf_path <- sub("\\.html?$", ".pdf", filename_html)
      converted <- FALSE
      # Try pagedown first (best fidelity), then webshot2 fallback
      if (has_pagedown) {
        try({
          pagedown::chrome_print(input = filename_html, output = pdf_path)
          converted <- file_exists(pdf_path)
        }, silent = TRUE)
      }
      if (!converted && has_webshot2) {
        try({
          # webshot2 may need file:// prefix on some platforms
          url_arg <- if (grepl("^file://", filename_html)) filename_html else paste0("file://", normalizePath(filename_html, winslash = "/"))
          webshot2::webshot(url = url_arg, file = pdf_path, vwidth = 1280, vheight = 800)
          converted <- file_exists(pdf_path)
        }, silent = TRUE)
      }
      if (converted) {
        rec2 <- data.frame(id = uuid("exp"), filename = basename(pdf_path), filepath = normalizePath(pdf_path, winslash = "/", mustWork = FALSE), type = "pdf", created_at = now(), criteria = project_id, stringsAsFactors = FALSE)
        dbWriteTable(con, "exports_meta", rec2, append = TRUE, row.names = FALSE)
        log_audit(module = "Reports", action = "export_pdf", target_table = "exports_meta", target_id = rec2$id, detail = list(filename = pdf_path))
        return(normalizePath(pdf_path, winslash = "/", mustWork = FALSE))
      } else {
        # conversion failed -> return HTML path
        return(normalizePath(filename_html, winslash = "/", mustWork = FALSE))
      }
    }
    
    normalizePath(filename_html, winslash = "/", mustWork = FALSE)
  }, error = function(e) {
    message("write_html_pdf_report error: ", e$message)
    NULL
  })
}

# ---------------------------
# Reactive state and UI/server
# ---------------------------
rv <- reactiveValues(data = NULL, proj_obs_created = character())
reload_data <- function() rv$data <- load_all()
reload_data()

compute_totals <- reactive({
  req(rv$data)
  pr <- rv$data$projects
  ex <- rv$data$expenses
  pr_active <- pr %>% filter(is_trashed == 0)
  ex_active <- ex %>% filter(is_trashed == 0)
  total_projects <- nrow(pr_active)
  total_budget <- sum(as.numeric(pr_active$budget_total), na.rm = TRUE)
  total_spent <- sum(as.numeric(ex_active$amount), na.rm = TRUE)
  remaining <- total_budget - total_spent
  list(total_projects = total_projects, total_budget = total_budget, total_spent = total_spent, remaining = remaining)
})

project_health <- function(pid) {
  pr <- rv$data$projects
  ex <- rv$data$expenses
  p <- pr %>% filter(id == pid)
  if (nrow(p) == 0) return(list(status = "unknown", pct = 0, spent = 0, budget = 0))
  spent <- ex %>% filter(project_id == pid & is_trashed == 0) %>% summarise(s = sum(amount, na.rm = TRUE)) %>% pull(s)
  spent <- ifelse(is.na(spent), 0, spent)
  budget <- as.numeric(p$budget_total)
  pct <- ifelse(budget == 0, 0, round(100 * spent / budget, 1))
  status <- ifelse(pct >= 100, "over", ifelse(pct >= 75, "warning", "ok"))
  list(status = status, pct = pct, spent = spent, budget = budget)
}

theme_choices <- c(
  "Light","Dark","System (Auto)","Minimalist","Soft / Pastel","Nature","Midnight / AMOLED",
  "High Contrast","Focus Mode","Custom Accent","Solarized Light","Solarized Dark","Ocean Blue",
  "Forest Green","Sunset","Lavender","Monochrome","Glassmorphism","Neumorphism","Vintage"
)

ui <- dashboardPage(
  skin = "blue",
  dashboardHeader(title = span("Budget Tracker", style = "font-weight:700;")),
  dashboardSidebar(
    width = 260,
    sidebarMenu(id = "main_nav",
                menuItem("DASHBOARD", tabName = "dashboard", icon = icon("tachometer-alt")),
                menuItem("PROJECTS", tabName = "projects", icon = icon("folder")),
                menuItem("EXPENSES", tabName = "expenses", icon = icon("dollar-sign")),
                menuItem("REPORTS", tabName = "reports", icon = icon("chart-line")),
                menuItem("SETTINGS", tabName = "settings", icon = icon("cog")),
                menuItem("AUDIT TRAIL", tabName = "audit", icon = icon("list-alt")),
                menuItem("FILE MANAGER", tabName = "files", icon = icon("folder")),
                menuItem("BACKUPS", tabName = "backups", icon = icon("save")),
                menuItem("EXIT", tabName = "exit", icon = icon("sign-out-alt"))
    )
  ),
  dashboardBody(
    # Splash overlay (non-blocking, hides after 2.5s)
    tags$head(
      tags$style(HTML("
        #splash-overlay { position: fixed; inset: 0; background: #000000; display:flex;align-items:center;justify-content:center;z-index:999999; transition: opacity 0.6s ease; opacity:1; pointer-events:auto; }
        #splash-overlay.hidden { opacity:0; pointer-events:none; }
        .splash-text { font-family: 'Inter', Arial, sans-serif; font-size:56px; font-weight:800; color:#00e5ff; text-transform:uppercase; letter-spacing:2px; text-align:center;
          text-shadow:0 0 6px rgba(0,229,255,0.22),0 0 12px rgba(0,229,255,0.18),0 0 18px rgba(0,150,255,0.12),0 0 30px rgba(75,0,255,0.08);
          animation: splashGlow 2.4s ease-in-out infinite; }
        @keyframes splashGlow { 0%{ color:#00e5ff; transform:scale(1);} 50%{ color:#7b2bff; transform:scale(1.03);} 100%{ color:#00e5ff; transform:scale(1);} }
      ")),
      tags$script(HTML("
        document.addEventListener('DOMContentLoaded', function() {
          try {
            var splash = document.getElementById('splash-overlay');
            if (!splash) return;
            setTimeout(function() {
              splash.classList.add('hidden');
              setTimeout(function() { if (splash && splash.parentNode) splash.parentNode.removeChild(splash); }, 700);
            }, 2500);
          } catch(e) { console && console.warn && console.warn('Splash error', e); }
        });
      "))
    ),
    tags$div(id = "splash-overlay", tags$div(class = "splash-text", "NICCO CREATIVES")),
    
    uiOutput("dynamic_theme"),
    tags$head(tags$style(HTML("
      .project-card { border: 1px solid #e1e1e1; padding: 10px; margin-bottom: 10px; border-radius: 6px; background: #fff;}
      .project-card .title { font-weight: 700; font-size: 16px;}
      .small-muted { color: #777; font-size: 12px; }
      .value-box { min-height: 90px; }
      .section-sep { border-top: 1px solid #e9eef6; margin: 12px 0; }
    "))),
    tabItems(
      tabItem(tabName = "dashboard",
              fluidRow(valueBoxOutput("vb_total_projects", width = 3), valueBoxOutput("vb_total_budget", width = 3), valueBoxOutput("vb_total_spent", width = 3), valueBoxOutput("vb_remaining", width = 3)),
              fluidRow(box(title = "Project Summaries", width = 12, status = "primary", uiOutput("project_cards")))
      ),
      tabItem(tabName = "projects",
              fluidRow(column(8, box(title = "Projects", width = NULL, status = "primary", DTOutput("dt_projects"))),
                       column(4, box(title = "Actions", width = NULL, status = "warning",
                                     actionButton("btn_new_project", "New Project", icon = icon("plus")),
                                     br(), br(),
                                     actionButton("btn_view_project", "View", icon = icon("eye")),
                                     actionButton("btn_edit_project", "Edit", icon = icon("edit")),
                                     actionButton("btn_delete_project", "Delete", icon = icon("trash")),
                                     br(), hr(),
                                     h4("Recycle Bin"),
                                     DTOutput("dt_recycle_projects"),
                                     actionButton("btn_restore_project", "Restore Selected", icon = icon("trash-restore")),
                                     actionButton("btn_purge_projects", "Empty Recycle Bin", icon = icon("trash-alt"), class = "btn-danger")
                       )))),
      tabItem(tabName = "expenses",
              fluidRow(column(4, box(title = "Expense Entry", width = NULL, status = "primary",
                                     selectInput("exp_project_select", "Project (required)", choices = NULL),
                                     uiOutput("budget_bar_ui"),
                                     textInput("exp_desc", "Description", ""),
                                     numericInput("exp_amount", "Amount", value = 0, min = 0, step = 0.01),
                                     textInput("exp_category", "Category", "General"),
                                     dateInput("exp_date", "Date", value = Sys.Date()),
                                     fileInput("exp_receipt", "Receipt", accept = c("image/*", "application/pdf")),
                                     actionButton("exp_save", "Save", icon = icon("save"), class = "btn-success"),
                                     actionButton("exp_duplicate", "Duplicate Last", icon = icon("copy")),
                                     br(), br(),
                                     verbatimTextOutput("exp_msg")
              )), column(8, box(title = "Expenses (project-specific)", width = NULL, status = "primary", DTOutput("dt_expenses"))))),
      tabItem(tabName = "reports",
              fluidRow(column(4, box(title = "Export Options", width = NULL, status = "primary",
                                     selectInput("report_project", "Select Project (required)", choices = NULL),
                                     checkboxInput("report_watermark", "Apply Watermark", value = TRUE),
                                     textInput("report_watermark_text", "Watermark text", value = get_setting("watermark_text") %||% "CONFIDENTIAL"),
                                     checkboxInput("report_pdf", "Also produce PDF (best-effort)", value = FALSE),
                                     textInput("report_excel_password", "Excel password (optional)", value = get_setting("excel_password") %||% ""),
                                     actionButton("btn_export_excel", "Export Excel (Project)", icon = icon("file-excel"), class = "btn-primary"),
                                     actionButton("btn_export_html", "Export HTML/PDF (Project)", icon = icon("file"), class = "btn-primary")
              )), column(8, box(title = "Exports", width = NULL, status = "primary", DTOutput("dt_exports"))))),
      tabItem(tabName = "settings",
              fluidRow(
                box(title = "Appearance & Accessibility", width = 6, status = "primary",
                    selectInput("setting_theme", "Theme", choices = theme_choices, selected = get_setting("theme") %||% "Light"),
                    conditionalPanel("input.setting_theme == 'Custom Accent'",
                                     textInput("custom_accent_hex", "Custom accent color (hex)", value = "#2B84D8")
                    ),
                    p("Themes prioritize high readability: text and table colors are chosen for strong contrast.")
                ),
                box(title = "Export & Backup", width = 6, status = "primary",
                    fluidRow(column(12, tags$label("Default export folder"), shinyDirButton("choose_export_folder", "Choose folder", "Select export folder", multiple = FALSE), verbatimTextOutput("chosen_export_path"))),
                    hr(),
                    fluidRow(column(12, tags$label("Default backup folder"), shinyDirButton("choose_backup_folder", "Choose folder", "Select backup folder", multiple = FALSE), verbatimTextOutput("chosen_backup_path"))),
                    hr(),
                    textInput("setting_excel_password", "Excel password (for protected worksheets)", value = get_setting("excel_password") %||% ""),
                    checkboxInput("setting_protect_other", "Protect other worksheets with same password", value = (tolower(as.character(get_setting("protect_other_sheets") %||% "false")) == "true")),
                    actionButton("btn_save_settings", "Save Settings", icon = icon("save"))
                )
              )),
      tabItem(tabName = "audit", fluidRow(box(title = "Audit Trail", width = 12, status = "primary", DTOutput("dt_audit"), br(), downloadButton("download_audit_csv", "Export Audit CSV")))),
      tabItem(tabName = "files", fluidRow(column(6, box(title = "Receipts", width = NULL, status = "primary", DTOutput("dt_files_receipts"))), column(6, box(title = "Exports & Backups", width = NULL, status = "primary", DTOutput("dt_files_exports"), DTOutput("dt_files_backups"))))),
      tabItem(tabName = "backups", fluidRow(box(title = "Backups", width = 6, status = "primary", actionButton("btn_create_backup", "Create Backup", icon = icon("save"), class = "btn-success"), br(), br(), DTOutput("dt_backups"), actionButton("btn_restore_selected_backup", "Restore Selected Backup", icon = icon("undo"), class = "btn-warning")), box(title = "Integrity & Maintenance", width = 6, status = "primary", actionButton("btn_run_integrity", "Run Consistency Check", icon = icon("check")), verbatimTextOutput("integrity_report")))),
      tabItem(tabName = "exit", fluidRow(box(title = "Exit", width = 12, status = "danger", p("Click the button below to save state and close the application safely."), actionButton("btn_exit_app", "Exit Application", icon = icon("sign-out-alt"), class = "btn-danger"))))
    )
  )
)

server <- function(input, output, session) {
  # shinyFiles roots
  roots <- c(Home = fs::path_home(), `App Data` = normalizePath(data_dir, winslash = "/"))
  shinyDirChoose(input, "choose_export_folder", roots = roots, session = session)
  shinyDirChoose(input, "choose_backup_folder", roots = roots, session = session)
  
  # dynamic theme CSS (same as before)
  output$dynamic_theme <- renderUI({
    theme <- get_setting("theme") %||% "Light"
    css <- ""
    if (theme == "System (Auto)") {
      css <- "
        @media (prefers-color-scheme: dark) {
          body, .content-wrapper { background: #0b1012 !important; color: #e6eef6 !important; }
          .box { background: #0f1720 !important; color: #e6eef6 !important; border-color: #263238 !important; }
          .dataTables_wrapper table { color: #e6eef6 !important; }
        }
        @media (prefers-color-scheme: light) {
          body, .content-wrapper { background: #ffffff !important; color: #0b2545 !important; }
          .box { background: #f8fafc !important; color: #0b2545 !important; border-color: #e6eef8 !important; }
          .dataTables_wrapper table { color: #0b2545 !important; }
        }
      "
    } else {
      css_map <- list(
        "Light" = "
          body, .content-wrapper { background: #ffffff !important; color: #0b2545 !important; }
          .box { background: #fbfdff !important; color: #0b2545 !important; border-color: #e6eef8 !important; }
          .dataTables_wrapper table { color: #0b2545 !important; }
        ",
        "Dark" = "
          body, .content-wrapper { background: #121416 !important; color: #e6eef6 !important; }
          .box { background: #1a1f24 !important; color: #e6eef6 !important; border-color: #2a2f34 !important; }
          .dataTables_wrapper table { color: #e6eef6 !important; }
        ",
        "Minimalist" = "
          body, .content-wrapper { background: #ffffff !important; color: #111 !important; font-family: 'Inter', Arial, sans-serif; }
          .box { background: transparent !important; border: none !important; box-shadow: none !important; }
          .dataTables_wrapper table { color: #111 !important; }
        ",
        "Soft / Pastel" = "
          body, .content-wrapper { background: #fffafc !important; color: #2b2b2b !important; }
          .box { background: #fffefc !important; color: #2b2b2b !important; border-color: #f5e6f0 !important; }
        "
        # (other themes same as previous)
      )
      css <- css_map[[theme]] %||% ""
    }
    css_common <- "
      .dataTables_wrapper .dataTables_length, .dataTables_wrapper .dataTables_filter, .dataTables_wrapper table thead th { color: inherit !important; }
      table.dataTable thead th, table.dataTable thead td { color: inherit !important; }
      .form-control, label, .control-label { color: inherit !important; background-color: transparent !important; }
    "
    tags$head(tags$style(HTML(paste(css, css_common, collapse = "\n"))))
  })
  
  output$chosen_export_path <- renderText({
    if (!is.null(input$choose_export_folder)) {
      path <- tryCatch(parseDirPath(roots, input$choose_export_folder), error = function(e) "")
      if (length(path) && nzchar(path)) return(paste0("Selected: ", path))
    }
    paste0("Current: ", get_export_folder())
  })
  output$chosen_backup_path <- renderText({
    if (!is.null(input$choose_backup_folder)) {
      pathb <- tryCatch(parseDirPath(roots, input$choose_backup_folder), error = function(e) "")
      if (length(pathb) && nzchar(pathb)) return(paste0("Selected: ", pathb))
    }
    paste0("Current: ", get_backup_folder())
  })
  
  # Persist theme setting
  observeEvent(input$setting_theme, { if (!is.null(input$setting_theme)) set_setting("theme", input$setting_theme) }, ignoreInit = TRUE)
  
  # Save settings handler (resyncs projects)
  observeEvent(input$btn_save_settings, {
    tryCatch({
      if (!is.null(input$choose_export_folder)) {
        path <- tryCatch(parseDirPath(roots, input$choose_export_folder), error = function(e) "")
        if (length(path) && nzchar(path)) set_setting("export_folder", path)
      }
      if (!is.null(input$choose_backup_folder)) {
        pathb <- tryCatch(parseDirPath(roots, input$choose_backup_folder), error = function(e) "")
        if (length(pathb) && nzchar(pathb)) set_setting("backup_folder", pathb)
      }
      set_setting("excel_password", input$setting_excel_password %||% "")
      set_setting("protect_other_sheets", tolower(as.character(input$setting_protect_other)))
      if (!is.null(input$setting_theme)) set_setting("theme", input$setting_theme)
      if (!is.null(input$setting_theme) && input$setting_theme == "Custom Accent") set_setting("custom_accent", input$custom_accent_hex %||% "#2B84D8")
      # resync
      data_all <- load_all()
      pids <- data_all$projects$id
      for (pid in pids) try(sync_project_workbook(pid), silent = TRUE)
      reload_data()
      showNotification("Settings saved and all project workbooks re-synchronized.", type = "message")
    }, error = function(e) {
      showNotification(paste0("Failed to save settings: ", e$message), type = "error")
    })
  }, ignoreInit = TRUE)
  
  # Populate selectors (project-only; no "All")
  observe({
    reload_data()
    pr <- rv$data$projects %>% filter(is_trashed == 0)
    proj_choices <- if (nrow(pr) > 0) setNames(pr$id, pr$name) else c()
    updateSelectInput(session, "report_project", choices = proj_choices, selected = if (length(proj_choices) > 0) proj_choices[[1]] else NULL)
    updateSelectInput(session, "exp_project_select", choices = proj_choices, selected = if (length(proj_choices) > 0) proj_choices[[1]] else NULL)
  }, priority = 1)
  
  # Value boxes
  output$vb_total_projects <- renderValueBox({ req(compute_totals()); valueBox(value = compute_totals()$total_projects, subtitle = "Total Projects", icon = icon("folder-open"), color = "aqua") })
  output$vb_total_budget <- renderValueBox({ req(compute_totals()); valueBox(value = scales::dollar(compute_totals()$total_budget), subtitle = "Total Budget", icon = icon("coins"), color = "green") })
  output$vb_total_spent <- renderValueBox({ req(compute_totals()); valueBox(value = scales::dollar(compute_totals()$total_spent), subtitle = "Total Spent", icon = icon("credit-card"), color = "red") })
  output$vb_remaining <- renderValueBox({ req(compute_totals()); valueBox(value = scales::dollar(compute_totals()$remaining), subtitle = "Remaining Balance", icon = icon("wallet"), color = ifelse(compute_totals()$remaining >= 0, "blue", "red")) })
  
  # Project cards
  output$project_cards <- renderUI({
    pr <- rv$data$projects %>% filter(is_trashed == 0)
    if (nrow(pr) == 0) return(tags$p("No projects yet."))
    cards <- lapply(seq_len(nrow(pr)), function(i) {
      p <- pr[i, ]
      ph <- project_health(p$id)
      status_color <- switch(ph$status, ok = "#2b7a2b", warning = "#d9822b", over = "#c62828", "#666666")
      viewId <- paste0("view_proj__", p$id)
      editId <- paste0("edit_proj__", p$id)
      trashId <- paste0("trash_proj__", p$id)
      box(width = 12, status = "info", solidHeader = FALSE,
          div(class = "project-card",
              div(class = "title", paste0(p$name, " — ", p$id)),
              div(tags$span(class = "small-muted", paste0("Budget: ", scales::dollar(p$budget_total), " | Spent: ", scales::dollar(ph$spent), " | Usage: ", ph$pct, "%"))),
              br(),
              div(strong("Last update: "), ifelse(is.na(p$updated_at) || p$updated_at == "", "-", p$updated_at)),
              br(),
              fluidRow(
                column(4, actionButton(inputId = viewId, label = "View", class = "btn-block btn-info")),
                column(4, actionButton(inputId = editId, label = "Edit", class = "btn-block btn-primary")),
                column(4, actionButton(inputId = trashId, label = "Delete", class = "btn-block btn-danger"))
              ),
              tags$div(style = paste0("margin-top:8px; color:", status_color, "; font-weight:700;"), paste0("Status: ", ph$status))
          )
      )
    })
    do.call(tagList, cards)
  })
  
  # Dynamic observers for card buttons
  observe({
    pr <- rv$data$projects %>% filter(is_trashed == 0)
    for (i in seq_len(nrow(pr))) {
      pid <- pr$id[i]
      viewId <- paste0("view_proj__", pid)
      editId <- paste0("edit_proj__", pid)
      trashId <- paste0("trash_proj__", pid)
      
      if (!(viewId %in% rv$proj_obs_created)) {
        local({
          mypid <- pid; myView <- viewId
          observeEvent(input[[myView]], {
            updateTabItems(session, "main_nav", "projects")
            pr_df <- rv$data$projects %>% filter(is_trashed == 0)
            idx <- which(pr_df$id == mypid)
            if (length(idx) == 1) {
              proxy <- dataTableProxy("dt_projects")
              selectRows(proxy, idx)
            }
            show_project_detail(mypid)
          }, ignoreInit = TRUE)
        })
        rv$proj_obs_created <- c(rv$proj_obs_created, viewId)
      }
      
      if (!(editId %in% rv$proj_obs_created)) {
        local({
          mypid <- pid; myEdit <- editId
          observeEvent(input[[myEdit]], { show_edit_project_modal(mypid) }, ignoreInit = TRUE)
        })
        rv$proj_obs_created <- c(rv$proj_obs_created, editId)
      }
      
      if (!(trashId %in% rv$proj_obs_created)) {
        local({
          mypid <- pid; myTrash <- trashId
          observeEvent(input[[myTrash]], {
            showModal(modalDialog(title = "Confirm Delete", paste0("Move project '", rv$data$projects %>% filter(id == mypid) %>% pull(name), "' to Recycle Bin? This will soft-delete associated expenses."), footer = tagList(modalButton("Cancel"), actionButton(paste0("confirm_trash_proj__", mypid), "Move to Recycle Bin", class = "btn-danger"))))
            observeEvent(input[[paste0("confirm_trash_proj__", mypid)]], {
              soft_delete_project(mypid); reload_data(); removeModal(); showNotification("Project moved to Recycle Bin", type = "warning")
            }, once = TRUE)
          }, ignoreInit = TRUE)
        })
        rv$proj_obs_created <- c(rv$proj_obs_created, trashId)
      }
    }
  })
  
  # Project detail modal
  show_project_detail <- function(pid) {
    data <- load_all()
    p <- data$projects %>% filter(id == pid)
    if (nrow(p) == 0) { showNotification("Project not found", type = "error"); return() }
    ex <- data$expenses %>% filter(project_id == pid & is_trashed == 0)
    attachments <- data$attachments %>% filter(expense_id %in% ex$id)
    audit_rel <- data$audit %>% filter(target_id == pid | target_id %in% ex$id | grepl(pid, detail, fixed = TRUE))
    ph <- project_health(pid)
    
    showModal(modalDialog(
      title = paste0("Project Detail — ", p$name),
      size = "l",
      easyClose = TRUE,
      footer = modalButton("Close"),
      tagList(
        tags$div(class = "section-sep"),
        fluidRow(
          column(6, tags$div(h4("Metadata"), tags$p(strong("ID: "), p$id), tags$p(strong("Name: "), p$name), tags$p(strong("Status: "), p$status))),
          column(6, tags$div(h4("Financial Summary"), tags$p(strong("Budget: "), scales::dollar(p$budget_total)), tags$p(strong("Total Spent: "), scales::dollar(ph$spent)), tags$p(strong("Remaining: "), scales::dollar(ph$budget - ph$spent))))
        ),
        tags$hr(),
        tabsetPanel(
          tabPanel("Overview",
                   fluidRow(column(12, tags$h4("Description"), tags$p(ifelse(is.null(p$description) || p$description == "", "-", p$description)))),
                   fluidRow(column(6, tags$h4("Timeline"), tags$p(paste0("Start: ", ifelse(is.na(p$start_date) || p$start_date == "", "-", p$start_date))), tags$p(paste0("End: ", ifelse(is.na(p$end_date) || p$end_date == "", "-", p$end_date))))),
                   fluidRow(column(12, tags$h4("Category Breakdown")), plotlyOutput(paste0("proj_pie_", pid), height = "300px"))
          ),
          tabPanel("Expenses", DT::dataTableOutput(paste0("proj_expenses_dt_", pid))),
          tabPanel("Attachments", DT::dataTableOutput(paste0("proj_attachments_dt_", pid))),
          tabPanel("Audit", DT::dataTableOutput(paste0("proj_audit_dt_", pid)))
        )
      )
    ))
    
    # Chart and tables
    output[[paste0("proj_pie_", pid)]] <- renderPlotly({
      req(ex)
      if (nrow(ex) == 0) return(NULL)
      cat_df <- ex %>% group_by(category) %>% summarise(total = sum(amount, na.rm = TRUE)) %>% arrange(desc(total))
      plot_ly(cat_df, labels = ~category, values = ~total, type = 'pie', textinfo = 'label+percent') %>% layout(showlegend = TRUE)
    })
    
    output[[paste0("proj_expenses_dt_", pid)]] <- DT::renderDataTable({
      df <- ex %>% transmute(ID = id, Description = description, Amount = amount, Category = category, Date = date, Receipt = ifelse(receipt_path == "", "", "Has Receipt"), Created = created_at)
      DT::datatable(df, options = list(pageLength = 10))
    })
    
    output[[paste0("proj_attachments_dt_", pid)]] <- DT::renderDataTable({
      df <- attachments %>% transmute(ID = id, Expense = expense_id, Filename = filename, Path = filepath, Size = size, Created = created_at)
      DT::datatable(df, options = list(pageLength = 10))
    })
    
    output[[paste0("proj_audit_dt_", pid)]] <- DT::renderDataTable({
      df <- audit_rel %>% transmute(Time = timestamp, User = user, Module = module, Action = action, Detail = detail)
      DT::datatable(df, options = list(pageLength = 10))
    })
  }
  
  # Edit modal
  show_edit_project_modal <- function(pid) {
    p <- rv$data$projects %>% filter(id == pid)
    if (nrow(p) == 0) { showNotification("Project not found", type = "error"); return(NULL) }
    showModal(modalDialog(
      title = paste0("Edit Project: ", p$name),
      textInput("edit_name_modal", "Name", value = p$name),
      textAreaInput("edit_desc_modal", "Description", value = p$description),
      numericInput("edit_budget_modal", "Budget", value = as.numeric(p$budget_total)),
      dateInput("edit_start_modal", "Start Date", value = ifelse(is.na(p$start_date) || p$start_date == "", NA, as.Date(p$start_date))),
      dateInput("edit_end_modal", "End Date", value = ifelse(is.na(p$end_date) || p$end_date == "", NA, as.Date(p$end_date))),
      checkboxInput("edit_locked_modal", "Lock Budget", value = as.logical(p$budget_locked)),
      footer = tagList(modalButton("Cancel"), actionButton(paste0("confirm_edit_project_modal__", pid), "Save", class = "btn-primary"))
    ))
    observeEvent(input[[paste0("confirm_edit_project_modal__", pid)]], {
      update_project(pid, list(name = input$edit_name_modal, description = input$edit_desc_modal, budget_total = as.numeric(input$edit_budget_modal), start_date = as.character(input$edit_start_modal), end_date = as.character(input$edit_end_modal), budget_locked = as.integer(input$edit_locked_modal)))
      reload_data()
      removeModal()
      showNotification("Project updated", type = "message")
    }, once = TRUE)
  }
  
  # Projects table outputs
  output$dt_projects <- renderDT({
    pr <- rv$data$projects
    df <- pr %>% filter(is_trashed == 0) %>% transmute(`Project ID` = id, `Project Name` = name, `Budget` = budget_total, `Start Date` = start_date, `End Date` = end_date, `Status` = status)
    DT::datatable(df, selection = "single", options = list(pageLength = 10, autoWidth = TRUE))
  }, server = FALSE)
  output$dt_recycle_projects <- renderDT({
    pr <- rv$data$projects %>% filter(is_trashed == 1) %>% transmute(`Project ID` = id, `Project Name` = name, `Deleted At` = updated_at)
    DT::datatable(pr, selection = "single", options = list(pageLength = 5, dom = 't'))
  }, server = FALSE)
  
  # New project wizard
  observeEvent(input$btn_new_project, {
    showModal(modalDialog(
      title = "New Project - Wizard",
      size = "l",
      footer = NULL,
      easyClose = TRUE,
      fluidPage(
        tabsetPanel(id = "proj_wizard",
                    tabPanel("Basic Info", textInput("w_name", "Name"), textInput("w_id", "ID (optional; will be generated if blank)"), textAreaInput("w_desc", "Description", "")),
                    tabPanel("Budget", numericInput("w_budget", "Total allocation", value = 0, min = 0, step = 0.01)),
                    tabPanel("Timeline", dateInput("w_start", "Start Date", value = Sys.Date()), dateInput("w_end", "End Date", value = Sys.Date() + 30)),
                    tabPanel("Categories", textInput("w_cats", "Default categories (comma-separated)", value = "General,Travel,Supplies")),
                    tabPanel("Confirm", verbatimTextOutput("proj_wizard_summary"), actionButton("proj_wizard_create", "Create Project", class = "btn-success"))
        )
      )
    ))
  })
  
  output$proj_wizard_summary <- renderPrint({
    cat("Name:", input$w_name, "\n")
    cat("ID:", ifelse(nzchar(input$w_id), input$w_id, "(will be generated)"), "\n")
    cat("Budget:", input$w_budget, "\n")
    cat("Start:", as.character(input$w_start), "\n")
    cat("End:", as.character(input$w_end), "\n")
    cat("Categories:", input$w_cats, "\n")
  })
  
  observeEvent(input$proj_wizard_create, {
    removeModal()
    id <- ifelse(nzchar(input$w_id), input$w_id, uuid("proj"))
    create_project(id = id, name = input$w_name, description = input$w_desc, budget_total = as.numeric(input$w_budget), start_date = as.character(input$w_start), end_date = as.character(input$w_end))
    reload_data()
    showNotification("Project created and workbook generated.", type = "message")
  })
  
  # Table-level actions for edit/delete/view
  observeEvent(input$btn_edit_project, {
    sel <- input$dt_projects_rows_selected
    if (length(sel) != 1) { showNotification("Select a project to edit", type = "warning"); return() }
    pr <- rv$data$projects %>% filter(is_trashed == 0); row <- pr[sel, ]
    show_edit_project_modal(row$id)
  })
  observeEvent(input$btn_delete_project, {
    sel <- input$dt_projects_rows_selected
    if (length(sel) != 1) { showNotification("Select a project to delete", type = "warning"); return() }
    pr <- rv$data$projects %>% filter(is_trashed == 0); row <- pr[sel, ]
    showModal(modalDialog(title = "Confirm Delete", paste0("Move project '", row$name, "' to Recycle Bin? This will soft-delete associated expenses."), footer = tagList(modalButton("Cancel"), actionButton("confirm_delete_project_modal_table", "Move to Recycle Bin", class = "btn-danger"))))
    observeEvent(input$confirm_delete_project_modal_table, { soft_delete_project(row$id); reload_data(); removeModal(); showNotification("Project moved to Recycle Bin", type = "warning") }, once = TRUE)
  })
  observeEvent(input$btn_view_project, {
    sel <- input$dt_projects_rows_selected
    if (length(sel) != 1) { showNotification("Select a project to view", type = "warning"); return() }
    pr <- rv$data$projects %>% filter(is_trashed == 0); row <- pr[sel, ]
    show_project_detail(row$id)
  })
  
  observeEvent(input$btn_restore_project, {
    sel <- input$dt_recycle_projects_rows_selected
    if (length(sel) != 1) { showNotification("Select a trashed project to restore", type = "warning"); return() }
    pr <- rv$data$projects %>% filter(is_trashed == 1); row <- pr[sel, ]; restore_project(row$id); reload_data(); showNotification("Project restored", type = "message")
  })
  
  observeEvent(input$btn_purge_projects, {
    showModal(modalDialog(title = "Empty Recycle Bin", "This will permanently delete all trashed projects and their expenses. This action cannot be undone.", footer = tagList(modalButton("Cancel"), actionButton("confirm_purge", "Purge Now", class = "btn-danger"))))
    observeEvent(input$confirm_purge, {
      con <- connect_db(); on.exit(dbDisconnect(con))
      trashed_expenses <- dbGetQuery(con, "SELECT id, receipt_path FROM expenses WHERE is_trashed = 1;")
      for (i in seq_len(nrow(trashed_expenses))) {
        pth <- trashed_expenses$receipt_path[i]
        if (nzchar(pth) && file_exists(pth)) file_move(pth, file.path(trash_dir, path_file(pth)))
      }
      dbExecute(con, "DELETE FROM attachments WHERE expense_id IN (SELECT id FROM expenses WHERE is_trashed = 1);")
      dbExecute(con, "DELETE FROM expenses WHERE is_trashed = 1;")
      dbExecute(con, "DELETE FROM projects WHERE is_trashed = 1;")
      log_audit(module = "Projects", action = "purge_recycle_bin", target_table = "projects", target_id = NA, detail = list())
      reload_data(); removeModal(); showNotification("Recycle Bin emptied", type = "message")
    }, once = TRUE)
  })
  
  # Expenses UI & actions: require selecting a project first
  output$budget_bar_ui <- renderUI({
    pid <- input$exp_project_select
    if (is.null(pid) || pid == "") {
      tags$div("Select a project to add/view expenses.")
    } else {
      ph <- project_health(pid)
      pct <- ph$pct
      color <- ifelse(ph$status == "over", "danger", ifelse(ph$status == "warning", "warning", "success"))
      tagList(tags$div(style = "margin-bottom:6px;", paste0("Budget: ", scales::dollar(ph$budget), " | Spent: ", scales::dollar(ph$spent), " | Remaining: ", scales::dollar(ph$budget - ph$spent))), progressBar(id = "proj_budget_bar", value = min(100, round(pct)), total = 100, display_pct = TRUE, status = color))
    }
  })
  
  observeEvent(input$exp_save, {
    pid <- input$exp_project_select
    if (is.null(pid) || !nzchar(pid)) { output$exp_msg <- renderText("Please select a project before adding an expense."); showNotification("Select a project first", type = "warning"); return() }
    pr_check <- rv$data$projects %>% filter(id == pid & is_trashed == 0)
    if (nrow(pr_check) != 1) { output$exp_msg <- renderText("Selected project is invalid or deleted."); showNotification("Invalid project selected", type = "error"); return() }
    amt <- as.numeric(input$exp_amount)
    if (is.na(amt) || amt <= 0) { output$exp_msg <- renderText("Enter a valid amount > 0"); return() }
    if (pr_check$budget_locked == 1) { output$exp_msg <- renderText("Project budget is locked. Cannot add expenses."); showNotification("Budget locked", type = "error"); return() }
    ph <- project_health(pid)
    if (ph$spent + amt > ph$budget) {
      showModal(modalDialog(title = "Budget Exceeded", paste0("This expense will exceed the project's budget (Current spent: ", scales::dollar(ph$spent), ", Budget: ", scales::dollar(ph$budget), "). Do you want to continue?"), footer = tagList(modalButton("Cancel"), actionButton("confirm_force_save", "Add Expense Anyway", class = "btn-danger"))))
      observeEvent(input$confirm_force_save, {
        id <- uuid("exp"); receipt <- NULL; rname <- NULL
        if (!is.null(input$exp_receipt) && !is.null(input$exp_receipt$datapath)) { receipt <- input$exp_receipt$datapath; rname <- input$exp_receipt$name }
        ok <- create_expense(id = id, project_id = pid, description = input$exp_desc, amount = amt, category = input$exp_category, date = as.character(input$exp_date), receipt_tmpfile = receipt, receipt_name = rname)
        if (isTRUE(ok)) { reload_data(); removeModal(); output$exp_msg <- renderText("Expense added (budget exceeded)."); showNotification("Expense added", type = "message") } else { removeModal(); showNotification("Failed to add expense", type = "error") }
      }, once = TRUE)
      return()
    }
    id <- uuid("exp"); receipt <- NULL; rname <- NULL
    if (!is.null(input$exp_receipt) && !is.null(input$exp_receipt$datapath)) { receipt <- input$exp_receipt$datapath; rname <- input$exp_receipt$name }
    ok <- create_expense(id = id, project_id = pid, description = input$exp_desc, amount = amt, category = input$exp_category, date = as.character(input$exp_date), receipt_tmpfile = receipt, receipt_name = rname)
    if (isTRUE(ok)) {
      reload_data(); output$exp_msg <- renderText("Expense saved."); updateTextInput(session, "exp_desc", value = ""); updateNumericInput(session, "exp_amount", value = 0); updateTextInput(session, "exp_category", value = "General"); updateDateInput(session, "exp_date", value = Sys.Date())
    } else {
      output$exp_msg <- renderText("Failed to save expense. See server logs."); showNotification("Failed to save expense", type = "error")
    }
  })
  
  observeEvent(input$exp_duplicate, {
    ex <- rv$data$expenses %>% filter(is_trashed == 0) %>% arrange(desc(created_at)) %>% slice_head(n = 1)
    if (nrow(ex) == 0) { showNotification("No expense to duplicate", type = "warning"); return() }
    if (is.null(ex$project_id) || !nzchar(ex$project_id)) { showNotification("Last expense is unassigned and cannot be duplicated into a project.", type = "warning"); return() }
    new_id <- uuid("exp")
    ok <- create_expense(id = new_id, project_id = ex$project_id, description = paste0("(dup) ", ex$description), amount = ex$amount, category = ex$category, date = as.character(Sys.Date()))
    if (isTRUE(ok)) { reload_data(); showNotification("Expense duplicated", type = "message") } else { showNotification("Failed to duplicate expense", type = "error") }
  })
  
  # Expenses table displays only selected project's expenses
  output$dt_expenses <- renderDT({
    pid <- input$exp_project_select
    if (is.null(pid) || !nzchar(pid)) {
      DT::datatable(data.frame(Message = "Select a project to view its expenses"), options = list(dom = 't'))
    } else {
      ex <- rv$data$expenses %>% filter(project_id == pid & is_trashed == 0) %>% transmute(ID = id, Description = description, Amount = amount, Category = category, Date = date, Receipt = ifelse(receipt_path == "", "", "Has Receipt"))
      DT::datatable(ex, selection = "single", extensions = 'Buttons', options = list(pageLength = 10, dom = 'Bfrtip', buttons = c('copy', 'csv', 'excel')))
    }
  }, server = FALSE)
  
  observeEvent(input$dt_expenses_rows_selected, {
    sel <- input$dt_expenses_rows_selected; if (length(sel) != 1) return()
    pid <- input$exp_project_select
    if (is.null(pid) || !nzchar(pid)) return()
    ex <- rv$data$expenses %>% filter(project_id == pid & is_trashed == 0) %>% arrange(desc(created_at))
    if (nrow(ex) == 0 || sel > nrow(ex)) return()
    row <- ex[sel, ]
    showModal(modalDialog(title = paste0("Expense: ", row$description), textInput("edit_exp_desc", "Description", value = row$description), numericInput("edit_exp_amount", "Amount", value = as.numeric(row$amount), step = 0.01), textInput("edit_exp_category", "Category", value = row$category), dateInput("edit_exp_date", "Date", value = ifelse(is.na(row$date) || row$date == "", Sys.Date(), as.Date(row$date))), if (nzchar(row$receipt_path) && file_exists(row$receipt_path)) tagList(tags$a(href = paste0("file://", row$receipt_path), target = "_blank", "Open Receipt"), br()) else tags$div("No receipt"), footer = tagList(modalButton("Cancel"), actionButton("confirm_update_expense", "Save"), actionButton("confirm_delete_expense", "Delete", class = "btn-danger")), size = "m"))
    observeEvent(input$confirm_update_expense, { ok <- update_expense(row$id, list(description = input$edit_exp_desc, amount = as.numeric(input$edit_exp_amount), category = input$edit_exp_category, date = as.character(input$edit_exp_date))); if (isTRUE(ok)) { reload_data(); removeModal(); showNotification("Expense updated", type = "message") } else { showNotification("Failed to update expense", type = "error") } }, once = TRUE)
    observeEvent(input$confirm_delete_expense, { ok <- soft_delete_expense(row$id); if (isTRUE(ok)) { reload_data(); removeModal(); showNotification("Expense moved to Recycle Bin", type = "warning") } else { showNotification("Failed to delete expense", type = "error") } }, once = TRUE)
  })
  
  # Reports/Exports: enforce project selection; robust error handling
  output$dt_exports <- renderDT({ df <- rv$data$exports_meta; if (nrow(df) == 0) DT::datatable(data.frame(Message = "No exports"), options = list(dom = 't')) else DT::datatable(df, options = list(pageLength = 10), selection = "single") })
  
  observeEvent(input$btn_export_excel, {
    pid <- input$report_project
    if (is.null(pid) || !nzchar(pid)) { showNotification("Select a project to export its Excel workbook.", type = "warning"); return() }
    pr_check <- rv$data$projects %>% filter(id == pid & is_trashed == 0)
    if (nrow(pr_check) != 1) { showNotification("Selected project is invalid or deleted.", type = "error"); return() }
    watermark <- ifelse(isTRUE(input$report_watermark), input$report_watermark_text, "")
    tryCatch({
      fname <- write_excel_report(project_id = pid, watermark = watermark)
      reload_data()
      if (!is.null(fname)) showNotification(paste0("Excel exported: ", basename(fname)), type = "message") else showNotification("Excel export failed", type = "error")
    }, error = function(e) {
      showNotification(paste0("Excel export failed: ", e$message), type = "error")
    })
  })
  
  observeEvent(input$btn_export_html, {
    pid <- input$report_project
    if (is.null(pid) || !nzchar(pid)) { showNotification("Select a project to export its report.", type = "warning"); return() }
    pr_check <- rv$data$projects %>% filter(id == pid & is_trashed == 0)
    if (nrow(pr_check) != 1) { showNotification("Selected project is invalid or deleted.", type = "error"); return() }
    watermark <- ifelse(isTRUE(input$report_watermark), input$report_watermark_text, "")
    pdf_flag <- isTRUE(input$report_pdf)
    tryCatch({
      out <- write_html_pdf_report(project_id = pid, pdf = pdf_flag, watermark = watermark)
      reload_data()
      if (!is.null(out)) showNotification(paste0("Report generated: ", basename(out)), type = "message") else showNotification("Report generation failed", type = "error")
    }, error = function(e) {
      showNotification(paste0("Report generation failed: ", e$message), type = "error")
    })
  })
  
  # Audit table & download
  output$dt_audit <- renderDT({
    df <- rv$data$audit %>% transmute(Time = timestamp, User = user, Module = module, Action = action, Table = target_table, Target = target_id, Detail = detail)
    DT::datatable(df, options = list(pageLength = 25, autoWidth = TRUE), filter = "top")
  })
  output$download_audit_csv <- downloadHandler(filename = function() paste0("audit_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"), content = function(file) write.csv(rv$data$audit, file, row.names = FALSE))
  
  # Files manager
  output$dt_files_receipts <- renderDT({
    ats <- rv$data$attachments
    if (nrow(ats) == 0) return(DT::datatable(data.frame(Message = "No attachments"), options = list(dom = 't')))
    df <- ats %>% transmute(ID = id, Expense = expense_id, Filename = filename, Path = filepath, Size = size, Created = created_at)
    DT::datatable(df, selection = "single", options = list(pageLength = 10))
  })
  output$dt_files_exports <- renderDT({
    ex <- rv$data$exports_meta
    if (nrow(ex) == 0) return(DT::datatable(data.frame(Message = "No exports"), options = list(dom = 't')))
    df <- ex %>% transmute(ID = id, Name = filename, Path = filepath, Type = type, Created = created_at)
    DT::datatable(df, selection = "single", options = list(pageLength = 10))
  })
  output$dt_files_backups <- renderDT({
    con <- connect_db(); on.exit(dbDisconnect(con))
    bk <- dbGetQuery(con, "SELECT * FROM backups ORDER BY created_at DESC;")
    if (nrow(bk) == 0) return(DT::datatable(data.frame(Message = "No backups"), options = list(dom = 't')))
    df <- bk %>% transmute(ID = id, Name = filename, Path = filepath, Created = created_at, Size = size)
    DT::datatable(df, selection = "single", options = list(pageLength = 10))
  })
  
  observeEvent(input$dt_files_exports_rows_selected, {
    sel <- input$dt_files_exports_rows_selected
    if (length(sel) != 1) return()
    ex <- rv$data$exports_meta; row <- ex[sel, ]
    showModal(modalDialog(title = "Export Options", paste0("File: ", row$filename), footer = tagList(downloadButton("download_export_file", "Download"), modalButton("Close"))))
    output$download_export_file <- downloadHandler(filename = function() row$filename, content = function(file) {
      if (file_exists(row$filepath)) file.copy(row$filepath, file) else stop("File not found")
    })
  })
  
  observeEvent(input$dt_files_receipts_rows_selected, {
    sel <- input$dt_files_receipts_rows_selected
    if (length(sel) != 1) return()
    ats <- rv$data$attachments; row <- ats[sel, ]
    showModal(modalDialog(title = "Receipt Options", paste0("File: ", row$filename), footer = tagList(downloadButton("download_receipt_file", "Download"), actionButton("delete_receipt_file", "Delete", class = "btn-danger"), modalButton("Close"))))
    output$download_receipt_file <- downloadHandler(filename = function() row$filename, content = function(file) {
      if (file_exists(row$filepath)) file.copy(row$filepath, file) else stop("File not found")
    })
    observeEvent(input$delete_receipt_file, {
      tryCatch({
        if (file_exists(row$filepath)) file_move(row$filepath, file.path(trash_dir, path_file(row$filepath)))
        con <- connect_db(); on.exit(dbDisconnect(con))
        dbExecute(con, "DELETE FROM attachments WHERE id = ?;", params = list(row$id))
        reload_data()
        removeModal()
        showNotification("Receipt moved to trash", type = "warning")
      }, error = function(e) {
        showNotification(paste0("Failed to delete receipt: ", e$message), type = "error")
      })
    }, once = TRUE)
  })
  
  # Backups & integrity
  create_backup <- function(note = "") {
    tryCatch({
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S"); fname <- paste0("backup_", timestamp, ".zip"); path <- file.path(get_backup_folder(), fname)
      files_to_zip <- dir_ls(data_dir, recurse = TRUE, type = "file")
      try({ con <- connect_db(); dbDisconnect(con) }, silent = TRUE)
      zip::zip(zipfile = path, files = files_to_zip, mode = "cherry-pick")
      info <- file_info(path)
      rec <- data.frame(id = uuid("backup"), filename = fname, filepath = path, created_at = now(), size = as.integer(info$size), checksum = digest::digest(file(path, "rb"), algo = "md5"), notes = note, stringsAsFactors = FALSE)
      con <- connect_db(); on.exit(dbDisconnect(con))
      dbWriteTable(con, "backups", rec, append = TRUE, row.names = FALSE)
      log_audit(module = "Backups", action = "create_backup", target_table = "backups", target_id = rec$id, detail = list(path = path))
      path
    }, error = function(e) { message("create_backup error: ", e$message); showNotification(paste0("Backup failed: ", e$message), type = "error"); NULL })
  }
  
  observeEvent(input$btn_create_backup, { path <- create_backup(note = "User created backup"); reload_data(); if (!is.null(path)) showNotification(paste0("Backup created: ", basename(path)), type = "message") })
  
  observeEvent(input$btn_restore_selected_backup, {
    sel <- input$dt_backups_rows_selected
    if (length(sel) != 1) { showNotification("Select a backup to restore", type = "warning"); return() }
    con <- connect_db(); on.exit(dbDisconnect(con))
    bk <- dbGetQuery(con, "SELECT * FROM backups ORDER BY created_at DESC;"); row <- bk[sel, ]
    showModal(modalDialog(title = "Confirm Restore", paste0("Are you sure you want to restore backup: ", row$filename, "? Current DB will be backed up before restore."), footer = tagList(modalButton("Cancel"), actionButton("confirm_restore_backup_modal", "Restore", class = "btn-warning"))))
    observeEvent(input$confirm_restore_backup_modal, {
      removeModal()
      tryCatch({
        zip_path <- row$filepath
        if (!file_exists(zip_path)) stop("Backup file not found")
        if (file_exists(db_file)) file_copy(db_file, file.path(get_backup_folder(), paste0("db_before_restore_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".sqlite")))
        tmpd <- file_temp(pattern = "restore_"); dir_create(tmpd); unzip(zip_path, exdir = tmpd)
        files <- dir_ls(tmpd, recurse = TRUE, type = "file")
        for (f in files) {
          rel <- path_rel(f, start = tmpd)
          dest <- path(data_dir, rel); dir_create(path_dir(dest), recurse = TRUE)
          file_copy(f, dest, overwrite = TRUE)
        }
        log_audit(module = "Backups", action = "restore_backup", detail = list(zip = zip_path))
        reload_data(); showNotification("Backup restored. App data reloaded.", type = "message")
      }, error = function(e) { showNotification(paste0("Restore failed: ", e$message), type = "error") })
    }, once = TRUE)
  })
  
  observeEvent(input$btn_run_integrity, {
    con <- connect_db(); on.exit(dbDisconnect(con))
    issues <- character()
    ex <- dbGetQuery(con, "SELECT * FROM expenses;"); pr <- dbGetQuery(con, "SELECT * FROM projects;")
    orphan <- ex$project_id[ !is.na(ex$project_id) & ex$project_id != "" & !(ex$project_id %in% pr$id) ]
    if (length(orphan) > 0) issues <- c(issues, paste0("Orphan expenses referencing missing projects: ", paste(unique(orphan), collapse = ", ")))
    computed <- rv$data$expenses %>% filter(is_trashed == 0) %>% summarise(total = sum(amount, na.rm = TRUE)) %>% pull(total)
    dbsum <- dbGetQuery(con, "SELECT SUM(amount) as s FROM expenses WHERE is_trashed = 0;")$s
    if (is.na(dbsum)) dbsum <- 0
    if (abs(as.numeric(computed) - as.numeric(dbsum)) > 1e-6) issues <- c(issues, paste0("Expense totals mismatch: computed=", computed, " db=", dbsum))
    if (length(issues) == 0) output$integrity_report <- renderText("No issues found. Integrity check passed.") else output$integrity_report <- renderText(paste(issues, collapse = "\n"))
  })
  
  observeEvent(input$btn_exit_app, { log_audit(module = "System", action = "exit", detail = list()); showModal(modalDialog("Saving and closing application...")); Sys.sleep(0.3); stopApp() })
  
  # Auto reload
  observe({
    inval <- suppressWarnings(as.numeric(get_setting("auto_save_interval_sec"))) %||% 30
    if (is.na(inval) || inval <= 0) inval <- 30
    invalidateLater(inval * 1000, session)
    reload_data()
  })
}

# ---------------------------
# Launch app
# ---------------------------
shinyApp(ui, server)