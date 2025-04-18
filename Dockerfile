FROM apache/airflow:2.7.1

USER root

# 安装 Java (Spark 运行需要)
RUN apt-get update && apt-get install -y openjdk-11-jdk curl

# 下载并安装 Spark
#RUN curl -O https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz \
    #&& tar -xvzf spark-3.3.0-bin-hadoop3.tgz \
    #&& mv spark-3.3.0-bin-hadoop3 /opt/spark \
    #&& rm spark-3.3.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:/opt/spark/bin"

# 切换到 airflow 用户，再安装 pip 包
USER airflow

#RUN pip install apache-airflow-providers-apache-spark==4.1.5


# 安装 BigQuery 客户端
RUN pip install google-cloud-bigquery==3.12.0