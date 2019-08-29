mysql -u root -p mars_airflow -e "
LOAD DATA LOCAL INFILE
'./config/connections.csv' INTO TABLE connections
FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES
TERMINATED BY '\n';
"
