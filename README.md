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
