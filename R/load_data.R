library(readr)
library(RSQLite)

# Load the datasets
category <- readr::read_csv("dataset/Category_v2.csv")
customer <- readr::read_csv("dataset/customer_v2.csv")
order1 <- readr::read_csv("dataset/order_v4.csv")
product <- readr::read_csv("dataset/product_v2.csv")
shipping <- readr::read_csv("dataset/shipping_v3.csv")
supplier <- readr::read_csv("dataset/supplier_v2.csv")

# Connect to the database
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "database/group_33.db")

# Append each data frame to its respective table in the database
# Adjust the table names as needed
RSQLite::dbWriteTable(my_connection, "category", category, append = TRUE)
RSQLite::dbWriteTable(my_connection, "customer", customer, append = TRUE)
RSQLite::dbWriteTable(my_connection, "order", order1, append = TRUE)
RSQLite::dbWriteTable(my_connection, "product", product, append = TRUE)
RSQLite::dbWriteTable(my_connection, "shipping", shipping, append = TRUE)
RSQLite::dbWriteTable(my_connection, "supplier", supplier, append = TRUE)

#check new data



# Close the database connection
RSQLite::dbDisconnect(my_connection)
