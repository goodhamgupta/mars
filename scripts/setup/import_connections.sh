#mysql -u root -p mars_airflow -e "
#LOAD DATA LOCAL INFILE
#'./config/connections.csv' INTO TABLE connections
#FIELDS TERMINATED BY ','
#ENCLOSED BY '"' LINES
#TERMINATED BY '\n';
#"
PGPASSWORD=mars psql -U mars -d mars -h 0.0.0.0 -c "
COPY connection from './config/connection.csv' delimiter ','
"
