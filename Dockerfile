# 1. Start with the official Airflow image
FROM apache/airflow:2.7.1

# 2. Switch to root user to install system tools (Java)
USER root
RUN apt-get update && \
    apt-get install -y default-jdk ant && \
    apt-get clean

# 3. Set JAVA_HOME variable so Spark can find it
ENV JAVA_HOME /usr/lib/jvm/default-java
RUN export JAVA_HOME

# 4. Switch back to airflow user to install Python libraries
USER airflow
RUN pip install pyspark fpdf