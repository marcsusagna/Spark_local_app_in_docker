FROM python:3.10-slim-buster

# Install Java necessary packages
RUN apt-get update
RUN apt-get install --yes default-jre
RUN apt-get install --yes default-jdk

# Install wget to fetch Spark
RUN apt-get install --yes wget
RUN apt-get clean

# Download and install spark (folder when it can interact with java's default installation folder)
ARG SPARK_VERSION=3.3.1
ARG HADOOP_VERSION=3
WORKDIR /tmp
RUN wget "https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz"
RUN tar xvf "spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz"
RUN mv "spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION" /usr/local

WORKDIR /usr/local

# Configure spark
ENV SPARK_HOME="/usr/local/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION"
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin


# Install python libraries
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

WORKDIR /spark_app

