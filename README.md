# Ground Data Service
This service provides access to measurement-data of sensors on the earth.

## Setup
When not running this service via docker compose like in [AIMLESS](https://git.scc.kit.edu/master-thesis-ai-ml-based-support-for-satellite-exploration/aimlsse), first create a volume:
```
docker volume create --name ground-data-storage
```
Then run the container and use the volume:
```
docker run -d -p 8000:8000 -v ground-data-storage:/aimlsse/app/data ground-data-service
```
Here `/aimlsse/app/` is the working directory of the service, while `data` is the subdirectory for the data that is specified in the `config.yml` file.
If the path in the config is changed, update the container-side binding of the volume in the command above as well.

## Notes
Currently, the real measurements are not accessed. The data is automatically generated when needed.
To keep the repository clean and due to generated data being negligible in the future, this data is not included.