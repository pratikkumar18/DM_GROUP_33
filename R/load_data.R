library(ggplot2)
library(dplyr)
library(readr)
library(RSQLite)
library(DBI)

# Load the original datasets
category <- readr::read_csv("dataset/Category/Category_v2.csv")
customer <- readr::read_csv("dataset/Customer/customer_v2.csv")
order <- readr::read_csv("dataset/Order/order_v4.csv")
product <- readr::read_csv("dataset/Product/product_v2.csv")
shipping <- readr::read_csv("dataset/Shipping/shipping_v3.csv")
supplier <- readr::read_csv("dataset/Supplier/supplier_v2.csv")

#change date data to character 
customer$Date_of_birth <- as.character(customer$Date_of_birth)
order$Order_date <- as.character(order$Order_date)
shipping$Shipping_date <- as.character(shipping$Shipping_date)

# Connect to the database
database <- dbConnect(RSQLite::SQLite(), dbname = "group_33.db")


# If CUSTOMER table exist, drop it
if(dbExistsTable(database, "CUSTOMER")){
  dbExecute(database, "DROP TABLE CUSTOMER")
}

#create CUSTOMER table
dbExecute(database, "CREATE TABLE 'CUSTOMER' (
                'Customer_id' CHAR(5) PRIMARY KEY,
                'First_name' VARCHAR NOT NULL,
                'Last_name' VARCHAR NOT NULL,
                'Gender' VARCHAR NOT NULL,
                'Date_of_birth' DATE NOT NULL,
                'Email' VARCHAR NOT NULL UNIQUE,
                'Phone_number' VARCHAR NOT NULL UNIQUE)")

# If ORDER table exist, drop it
if(dbExistsTable(database, "ORDER")){
  dbExecute(database, "DROP TABLE 'ORDER'")
}

#create ORDER table
dbExecute(database, "CREATE TABLE 'ORDER' (
                'Order_id' CHAR(6) NOT NULL,
                'Customer_id' CHAR(5) NOT NULL,
                'Product_id' CHAR(6) NOT NULL,
                'Shipping_id' CHAR(6) NOT NULL,
                'Order_date' DATE NOT NULL,
                'Quantity' INT NOT NULL,
                'Promotion_description' VARCHAR,
                'Discount_percent' DECIMAL, 
                'Card_number' VARCHAR(16) NOT NULL,
                'Card_scheme' VARCHAR NOT NULL,
                'Card_type' VARCHAR NOT NULL,
                'Security_code' CHAR(3) NOT NULL,
                'Cardholder_name' VARCHAR NOT NULL,
                'YY' CHAR(2) NOT NULL,
                'MM' CHAR(2) NOT NULL,
                PRIMARY KEY ('Order_id', 'Customer_id', 'Product_id', 'Shipping_id'),
                FOREIGN KEY ('Customer_id') REFERENCES CUSTOMER ('Customer_id'),
                FOREIGN KEY ('Product_id') REFERENCES PRODUCT ('Product_id'),
                FOREIGN KEY ('Shipping_id') REFERENCES SHIPPING ('Shipping_id'))")

# If SHIPPING table exist, drop it
if(dbExistsTable(database, "SHIPPING")){
  dbExecute(database, "DROP TABLE SHIPPING")
}

#create SHIPPING table
dbExecute(database, "CREATE TABLE 'SHIPPING' (
                'Shipping_id' CHAR(6) PRIMARY KEY,
                'Shipper_name' VARCHAR NOT NULL,
                'Shipping_status' VARCHAR NOT NULL,
                'Shipping_date' DATE NOT NULL,
                'Address_line_1' VARCHAR NOT NULL,
                'Postcode' VARCHAR NOT NULL,
                'City' VARCHAR NOT NULL,
                'Country' VARCHAR NOT NULL)")

# If PRODUCT table exist, drop it
if(dbExistsTable(database, "PRODUCT")){
  dbExecute(database, "DROP TABLE PRODUCT")
}

#create PRODUCT table
dbExecute(database, "CREATE TABLE 'PRODUCT' (
                'Product_id' CHAR(6) PRIMARY KEY,
                'Product_name' VARCHAR NOT NULL,
                'Product_description' VARCHAR,
                'Unit_price' DECIMAL NOT NULL,
                'Stock' INT NOT NULL, 
                'Supplier_id' CHAR(6) NOT NULL,
                'Category_id' CHAR(6) NOT NULL,
                FOREIGN KEY ('Supplier_id') REFERENCES SUPPLIER ('Supplier_id'),
                FOREIGN KEY ('Category_id') REFERENCES CATEGORY ('Category_id'))")

# If SUPPLIER table exist, drop it
if(dbExistsTable(database, "SUPPLIER")){
  dbExecute(database, "DROP TABLE SUPPLIER")
}

#create SUPPLIER table
dbExecute(database, "CREATE TABLE 'SUPPLIER' (
                'Supplier_id' CHAR(6) PRIMARY KEY,
                'Supplier_phone' VARCHAR NOT NULL,
                'Supplier_name' VARCHAR NOT NULL)")

# If CATEGORY table exist, drop it
if(dbExistsTable(database, "CATEGORY")){
  dbExecute(database, "DROP TABLE CATEGORY")
}
dbExecute(database, "CREATE TABLE 'CATEGORY' (
                'Category_id' CHAR(6) PRIMARY KEY,
                'Category_name' VARCHAR NOT NULL,
                'Parent_category' VARCHAR,
                'Parent_category_id' CHAR(6))")


# Append each data frame to its respective table in the database
# Adjust the table names as needed
dbWriteTable(database, "CUSTOMER", customer, append = TRUE)
dbWriteTable(database, "ORDER", order, append = TRUE)
dbWriteTable(database, "SHIPPING", shipping, append = TRUE)
dbWriteTable(database, "PRODUCT", product, append = TRUE)
dbWriteTable(database, "SUPPLIER", supplier, append = TRUE)
dbWriteTable(database, "CATEGORY", category, append = TRUE)


#update data
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

#plot graph
# Proportion of customers by gender
customer_data <- customer %>%
  group_by(Gender) %>%
  count() %>%
  ungroup() %>%
  mutate(percentage = `n` / sum(`n`)) %>%
  mutate(percent_label = scales::percent(percentage))

plot1 <- ggplot(customer_data, aes(x = "", y = percentage, fill = Gender)) +
  geom_col(color = "black") +
  geom_label(aes(label = percent_label), color = c(1, 1),
             position = position_stack(vjust = 0.5),
             show.legend = FALSE) +
  guides(fill = guide_legend(title = "Gender")) +
  coord_polar(theta = "y") +
  labs(title = "Proportion of Customers by Gender") +
  theme_void()

#save the plot to "figures" directory
ggsave("figures/customer_gender_proportion.png", plot = plot1)

#Top 10 Customers with the Highest Purchase Frequency

#connect to database
database <- dbConnect(RSQLite::SQLite(), dbname = "group_33.db")

top_purchased_customer <- RSQLite::dbGetQuery(database, "SELECT c.Customer_id, c.First_name, c.Last_name, oc.Order_Count
FROM (
    SELECT Customer_id, COUNT(DISTINCT Order_id) AS Order_Count
    FROM 'ORDER'
    GROUP BY Customer_id
    ORDER BY COUNT(DISTINCT Order_id) DESC
    LIMIT 10
) AS oc
JOIN CUSTOMER c ON oc.Customer_id = c.customer_id;")

plot2 <- ggplot(top_purchased_customer, aes(x=reorder(Customer_id, Order_Count), y=Order_Count)) +
  geom_bar(stat="identity", fill="skyblue") +
  coord_flip() + # Flips the axes
  labs(title="Top 10 Customers with the Highest Purchase Frequency", x="Customer ID", y="Total Purchase Times") +
  theme_minimal()

#save the plot to "figures" directory
ggsave("figures/Top10_customer_with_highest_purchase_frequency.png", plot = plot2)

# Top 10 Customers with the Highest Order Value

top_order_value_customer <- RSQLite::dbGetQuery(database, "SELECT
    c.Customer_id,
    SUM((o.Quantity * p.Unit_price) * (1 - COALESCE(o.Discount_percent, 0))) AS Total_Order_Value
FROM
    CUSTOMER c
JOIN
    'ORDER' o ON c.Customer_id = o.Customer_id
JOIN
    PRODUCT p ON o.Product_id = p.Product_id
GROUP BY
    c.Customer_id, c.First_name, c.Last_name
ORDER BY
    Total_Order_Value DESC
LIMIT 10;")

plot3 <- ggplot(top_order_value_customer, aes(x=reorder(Customer_id, Total_Order_Value), y=Total_Order_Value)) +
  geom_bar(stat="identity", fill="skyblue") +
  coord_flip() + # Flips the axes
  labs(title="Top 10 Customers with the Highest Order Value", x="Customer ID", y="Total Order Value") +
  theme_minimal()

#save the plot to "figures" directory
ggsave("figures/Top10_Customers_with_the_highest_order_value.png", plot = plot3)


# Disconnect from the database
RSQLite::dbDisconnect(database)

