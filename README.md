# Mars

Mars contains our in-house scripts to perform ETL on data from events and create custom derived tables/views.

## Installation

Mars is dependant on airflow. You can install all the dependencies using:

```bash
pip3 install -r requirements.txt
```

In the file `airflow.cfg`, make sure you change the following paths:

- `dags_folder`
- `base_log_folder`
- `sql_alchemy_conn`: This is optional.

Install the custom packages:

- Go into the hiveql folder and type: `pip3 install -e .`
- Similarly, go into the common folder in `dags/common` and type: `pip3 install -e .`

Start the airflow server and scheduler in seperate windows using the commands:
```bash
airflow webserver
airflow scheduler
```

Open the url `http://localhost:8080` to access the airflow web console.

Next, we need to import the variables needed for the DAG's. Click on the Admin option, and select Variables. Import the `config/variables.json` file present in the repository.

Finally, we need to add the connections(DB and EMR). The connections required are:

- aws_default
- emr_default
- airflow_db
- prod_mysql_milkyway

The setup script is present in `scripts/setup/import_connection.sh`. Update your mysql authorization in the script and execute it using:
```bash
bash import_connection.sh
```

Make sure you update your DB credentials and AWS credentials for the connection to work.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Origin of the name

A data pipeline directs the flow of data from source DBs to reporting DBs. The 
word _tenor_ can be used to describe movement, or flow, in a particular direction. 
Tenor is also a tone in music: the highest natural adult male singing voice. 
Incidentally, Johannes Kepler, the German astronomer who's best known for 
defining laws of planetary motion assigned musical notes to planets based on the 
difference between the maximum and minimum angular speeds of a planet. Mars 
happens to have the tenor note according to Keplar. Hence, Mars it is.
