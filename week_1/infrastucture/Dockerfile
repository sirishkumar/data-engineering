FROM python:3.9.1

RUN apt update && apt install -y wget
RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]