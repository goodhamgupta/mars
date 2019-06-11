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

## License
[MIT](https://choosealicense.com/licenses/mit/)
