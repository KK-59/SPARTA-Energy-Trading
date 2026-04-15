# SPARTA-Energy-Trading
Automating the calculation of cross-country spread electricity prices in Europe

The Project

Building a database that continually monitors energy markets in real time, across chosen zones in Europe and calculates useful information such as cross-border price spreads as screening signals for potential trading opportunities, bidding zones within countries (Norway and Sweden), and peak/off-peak spreads. This can be combined with more information, such as interconnector utilisation rate, whose capacity varies hour-to-hour, and generation mix, to be an indication of the reason for a price spread. Further, when prices are negative (briefly), it can be an indication of oversupply (often occurs in Germany on windy days) – detecting and logging this can be useful. 
The Need

While the data mentioned above is open-source, there is currently no tool on the market that provides a continuously updated database for this information. While there has been extensive academic work to analyse European power spreads and generation dynamics, these use historical data snapshots instead of real-time information. Having real-time information allows the user to track recurring spreads and predict price changes based on certain conditions. As the data available is incredibly granular, these can be highly optimised. 
Features

Data layer: holds day-ahead and intraday prices, generation mix, cross-border flows, interconnector capacity, timestamps, country/zone

Calculation layer: calculates cross-border spread calculation for all monitored zone pairs, interconnector utilisation rate (actual flow/max capacity), negative price detection

Output layer: a SQL-queryable database, daily summaries, sending alerts when a spread value exceeds a certain threshold, dashboard showing spread levels at a given time interval
Tech Stack


Enstoe API for fetching data – provides day-ahead data for European countries and zones, generation per production type, etc. 
SQLite or PostgreSQL and TimescaleDB (which allows for more with time-dependent data) – need to decide based on time constraints and future scaling 
Three-week timeline

Week 1: research & finalising features (1-2 days), setting up database (1 day), learning to use Enstoe, starting to code computations and database updating (2-3 days)

Week 2: Finishing coding computations and database updating, starting to code output, alerts, summaries, making the database query-able/usable

Week 3: Integrating everything, finalising, write-up

Post-internship: monitoring the platform, evaluating its results and use. 

