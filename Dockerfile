FROM python:3.8-slim-buster


# Obtain JRE/JDK and other necessary packages
RUN apt-get update
#RUN apt-get install --yes --no-install-recommends openjdk-11-jre-headless
RUN apt-get install --yes default-jre
RUN apt-get install --yes default-jdk
RUN apt-get install --yes wget
RUN apt-get clean

# Install spark
WORKDIR /tmp
RUN wget https://dlcdn.apache.org/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz
RUN tar xvf spark-3.2.2-bin-hadoop3.2.tgz
RUN mv spark-3.2.2-bin-hadoop3.2 /usr/local


WORKDIR /usr/local

# Configure spark

ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN ln -s spark-3.2.2-bin-hadoop3.2 spark

# Install python libraries
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt


#RUN "${SPARK_HOME}/sbin/start-master.sh"

WORKDIR /spark_app

