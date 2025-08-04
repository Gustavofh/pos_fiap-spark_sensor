FROM bitnami/spark:4.0.0-debian-12-r9

# Ivy requer um diretório HOME absoluto; usar root simplifica
USER 0
RUN mkdir -p /root/.ivy2

# Copia o job para dentro da imagem
COPY job_generator.py /opt/spark/job_generator.py

ENTRYPOINT ["spark-submit", \
       "--conf", "spark.eventLog.enabled=true", \
       "--conf", "spark.eventLog.dir=file:///opt/spark-events", \
       "/opt/spark/job_generator.py"]