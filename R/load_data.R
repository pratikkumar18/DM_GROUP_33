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


# Disconnect from the database
RSQLite::dbDisconnect(database)

