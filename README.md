# Ground Data Service
This service provides access to measurement-data of sensors on the earth.

## How to run
For now, this remains a manual task.
To start the service navigate into the root directory of this repository and enter the following command into a terminal
```
uvicorn ground_data_service.main:app --port 8000 --reload
```

## Notes
Currently, the measurements are not accessed.
Therefore a substitution `ground_measurements.csv` has to be created using the Jupyter-Notebook `sensor_data_generator.ipynb`. The data is then moved into a `data/` folder under this modules' root directory.
To keep the repository clean and due to generated data being negligible in the future, this data is not included.