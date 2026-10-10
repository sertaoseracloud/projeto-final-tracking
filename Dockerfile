# Imagem única do projeto: gerador, testes e pipeline PySpark.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /projeto
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Conector s3a: hadoop-aws na mesma versão do Hadoop embutido no PySpark 3.5.3 (3.3.4),
# mais o bundle Java que ele exige. Vão para a pasta de jars do próprio PySpark.
ARG HADOOP_AWS=3.3.4
ARG AWS_JAVA_SDK_BUNDLE=1.12.262
RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && curl -fsSL -o "$JARS/hadoop-aws-$HADOOP_AWS.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS/hadoop-aws-$HADOOP_AWS.jar" \
 && curl -fsSL -o "$JARS/aws-java-sdk-bundle-$AWS_JAVA_SDK_BUNDLE.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_JAVA_SDK_BUNDLE/aws-java-sdk-bundle-$AWS_JAVA_SDK_BUNDLE.jar"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python
