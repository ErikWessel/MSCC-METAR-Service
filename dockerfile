FROM aimlsse-base:latest

WORKDIR /aimlsse/app

COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD uvicorn ground_data_service.main:app --host 0.0.0.0 --port 8000

EXPOSE 8000