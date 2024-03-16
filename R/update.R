library(dplyr)
library(readr)
library(RSQLite)
library(DBI)

# Connect to the database
database <- dbConnect(RSQLite::SQLite(), dbname = "group_33.db")

#read new data
# Function to append new files to database
append_new_data <- function(directory, table_name, connection) {
  # Define the path for the log of processed files
  log_file_path <- paste0(directory, "/processed_files.log")
  
  # Read the log file to get a list of processed files, if it exists
  if (file.exists(log_file_path)) {
    processed_files <- readLines(log_file_path)
  } else {
    processed_files <- character(0)
  }
  
  # List all CSV files in the directory
  all_files <- list.files(directory, full.names = TRUE, pattern = "\\.csv$")
  
  # Update log to only include existing files
  processed_files <- processed_files[processed_files %in% all_files]
  
  # Determine new files by excluding processed files
  new_files <- setdiff(all_files, processed_files)
  
  # Process each new file
  for (file in new_files) {
    # Read data from the new file
    new_data <- readr::read_csv(file)
    
    # Append data to the database
    RSQLite::dbWriteTable(connection, table_name, new_data, append = TRUE, overwrite=FALSE)
  }
  
  # Update the log file with the current list of processed files
  writeLines(c(processed_files, new_files), log_file_path)
}

# Define the list of directories and corresponding table names
directories <- c("Category", "Customer", "Order", "Product", "Shipping", "Supplier")
table_names <- c("CATEGORY", "CUSTOMER", "ORDER", "PRODUCT", "SHIPPING", "SUPPLIER")

# Iterate over each directory and append new data
for (i in seq_along(directories)) {
  append_new_data(paste0("dataset/", directories[i]), table_names[i], database)
}

dbGetQuery(database, "SELECT * FROM PRODUCT")

# Disconnect from the database
RSQLite::dbDisconnect(database)

save.image("my_workspace.RData")
