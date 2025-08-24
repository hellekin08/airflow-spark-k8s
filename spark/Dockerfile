# Use the image you specified
FROM docker.io/library/spark:4.0.0

# Become root to install OS packages
USER root

# Make sure Python + pip are available (some Spark images don’t include them)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Environment so PySpark uses python3
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# (Usually already set in Spark images, but harmless)
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# App directory
WORKDIR /opt/app

# Add and install Python deps (if any). The conditional avoids failing on empty files.
COPY requirements.txt /opt/requirements.txt
RUN bash -lc 'if [ -s /opt/requirements.txt ]; then python3 -m pip install --no-cache-dir -r /opt/requirements.txt; fi'

# Add your PySpark script
COPY spark_job.py /opt/app/spark_job.py

# Drop privileges
RUN useradd -ms /bin/bash appuser && chown -R appuser:appuser /opt/app
USER appuser

# Default command runs the ETL; you can override args at `docker run` time
ENTRYPOINT ["spark-submit", "/opt/app/spark_job.py"]
# Default output goes to /data; mount a volume there
CMD ["--output", "/data/sales_by_product"]
