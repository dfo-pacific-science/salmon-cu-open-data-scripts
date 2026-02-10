################################################################################
# Title: sub_folders.R
# Author: Erika Anderson
# Date Created: Feb 2026

# Description:
# This script moves csv files from one output folder into folders
# uses keywords in filename to allocate to sub folders

# The source data comes from running the cu scripts first
# they produce 48 files in the output folder

# Output:
# each csv file is put into their corresponding subfolder for Open Data

# Usage:
# 1. Ensure the csv files are present in the output folder
# 2. Run the script to slect and move the files
# 3. CSVs will be moved to the sub folders
################################################################################



# ---- Settings ----
root_dir <- "output"   # Folder containing all CSVs
dry_run  <- FALSE       # Set TRUE to preview actions without moving files
overwrite <- FALSE        # If FALSE, skip when target file already exists

# Map: keyword (regex or literal) -> subfolder name
# Order matters: first matching key is used
rules <- c(
  "CK_CU"   = "Chinook_Salmon_CU",
  "CK_SBC"      = "Chinook_Southern_BC_CU",
  "CM"      = "Chum_Salmon_CU",
  "CO"   = "Coho_Salmon_CU",
  "PKE"      = "Pink_Even_Year_Salmon_CU",
  "PKO"       = "Pink_Odd_Year_Salmon_CU",
  "SEL"       = "Sockeye_Lake_Type_Salmon_CU",
  "SER"   = "Sockeye_River_Type_Salmon_CU"
)

# ---- Script ----
csvs <- list.files(root_dir, pattern = "\\.csv$", full.names = TRUE, ignore.case = TRUE)

plan <- lapply(csvs, function(f) {
  fname <- basename(f)
  match <- NULL
  for (key in names(rules)) {
    if (grepl(key, fname, ignore.case = TRUE, perl = TRUE)) {
      match <- key
      break
    }
  }
  if (is.null(match)) {
    return(list(src = f, dst = NA_character_, reason = "no_rule_match"))
  }

  date_suffix <- format(Sys.Date(), "%Y%m%d")
  subdir <- file.path(root_dir, paste0(rules[[match]], "_", date_suffix))
  dst <- file.path(subdir, fname)
  list(src = f, dst = dst, reason = "ok")
})

# Create a data.frame for easy review
plan_df <- do.call(rbind, lapply(plan, as.data.frame))
plan_df$exists <- ifelse(is.na(plan_df$dst), NA, file.exists(plan_df$dst))

# Show what will happen
print(plan_df[, c("src", "dst", "reason", "exists")], row.names = FALSE)

# Perform the move
if (!dry_run) {
  by_subdir <- split(plan_df, plan_df$dst)
  unique_subdirs <- unique(dirname(na.omit(plan_df$dst)))
  invisible(lapply(unique_subdirs, function(d) if (!dir.exists(d)) dir.create(d, recursive = TRUE)))

  for (i in seq_len(nrow(plan_df))) {
    src <- plan_df$src[i]
    dst <- plan_df$dst[i]

    if (is.na(dst)) {
      message("Skipping (no rule): ", src)
      next
    }
    if (file.exists(dst) && !overwrite) {
      message("Skipping (exists): ", dst)
      next
    }
    ok <- file.rename(src, dst)
    if (!ok) {
      # Fallback: file.copy + file.remove works across filesystems
      if (file.copy(src, dst, overwrite = overwrite)) {
        file.remove(src)
        message("Moved via copy/remove: ", basename(src), " -> ", dst)
      } else {
        warning("Failed to move: ", src, " -> ", dst)
      }
    } else {
      message("Moved: ", basename(src), " -> ", dst)
    }
  }
} else {
  message("Dry run enabled: no files moved.")
}