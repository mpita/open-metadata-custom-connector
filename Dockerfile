FROM docker.getcollate.io/openmetadata/ingestion:1.6.6

# Let's use the same workdir as the ingestion image
WORKDIR ingestion
USER airflow

# Install our custom connector
COPY custom-connector/connector connector
COPY custom-connector/setup.py .

RUN pip install --no-deps .