FROM python:3.11-slim-bullseye

WORKDIR /aimlsse/lib
RUN python -m pip install --no-cache-dir --trusted-host host.docker.internal --extra-index-url http://host.docker.internal:8060 aimlsse-api



WORKDIR /aimlsse/app
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD uvicorn ground_data_service.main:app --host 0.0.0.0 --port 8000

EXPOSE 8000