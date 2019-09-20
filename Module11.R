install.packages("DBI")

install.packages("RSQLite")

library(DBI)

library(RSQLite)

setwd("/Users/navjot/Documents/R/Module/Module11/")


# Download the SQLite database file I'm hosting on GitHub

download.file("https://github.com/KristenKehrer/datamovesme-sqlcourse/raw/gh-pages/assets/sqlcourse.db",
              "./sqlcourse.db", mode = "wb")

# Open a connection to the SQLite database

con <- dbConnect(RSQLite::SQLite(), "./sqlcourse.db")

## List all of the tables

dbListTables(con)

## read the salescall table

dbReadTable(con, "salescall")

# Run the following query:

queryResult <- dbSendQuery(con, "select max(customer_id) from salescall")

dbFetch(queryResult)




# Run the following query:

queryResult <- dbSendQuery(con, "select max(customer_id) from salescall")

dbFetch(queryResult)

