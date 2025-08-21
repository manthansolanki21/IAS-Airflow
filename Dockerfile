FROM apache/airflow:2.7.1-python3.11

USER root

# Install OpenJDK 11 (x86) and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        gcc \
        python3-dev \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    apache-airflow-providers-apache-spark==4.3.0 \
    pyarrow==21.0.0 \
    uuid==1.30

# RUN pip install --no-cache-dir \
#     apache-airflow-providers-google==17.1.0
