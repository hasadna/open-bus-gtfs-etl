# Pulled June 16, 2021
FROM python:3.8@sha256:c7706b8d1b1e540b9dd42ac537498d7f3138e4b8b89fb890b2ee4d2c0bccc8ea
RUN pip install --upgrade pip
WORKDIR /srv
RUN pip install -r https://raw.githubusercontent.com/hasadna/open-bus-stride-db/main/requirements.txt &&\
    git clone https://github.com/hasadna/open-bus-stride-db.git &&\
    pip install -e open-bus-stride-db
COPY requirements.txt ./open-bus-gtfs-etl/requirements.txt
RUN pip install -r open-bus-gtfs-etl/requirements.txt
COPY setup.py ./open-bus-gtfs-etl/setup.py
COPY open_bus_gtfs_etl ./open-bus-gtfs-etl/open_bus_gtfs_etl
RUN pip install -e open-bus-gtfs-etl
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["open-bus-gtfs-etl"]
CMD ["--help"]