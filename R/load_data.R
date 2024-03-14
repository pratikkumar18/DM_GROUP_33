library(readr)
library(RSQLite)

category <- readr::read_csv("dataset/Category_v2.csv")
customer <- readr::read_csv("dataset/customer_v2.csv")
newproduct <- readr::read_csv("dataset/new_product.csv")
order1 <- readr::read_csv("dataset/order_v4.csv")
product <- readr::read_csv("dataset/product_v2.csv")
shipping <- readr::read_csv("dataset/shipping_v3.csv")
supplier <- readr::read_csv("dataset/supplier_v2.csv")

my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/group_33.db")
RSQLite::dbWriteTable(my_connection,"Schema",category,customer,newproduct,order1,product,shipping,supplier)

