library(dplyr)
library(readr)
library(RSQLite)
library(DBI)

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

# Disconnect from the database
RSQLite::dbDisconnect(database)