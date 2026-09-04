FROM spark:4.1.2-scala2.13-java17-python3-ubuntu

USER root
RUN pip install --no-cache-dir "psycopg[binary]==3.3.5"
COPY src /opt/spark/project/src
USER spark
