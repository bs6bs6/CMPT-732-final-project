FROM ubuntu:20.04

LABEL org.opencontainers.image.authors="bs6"
# avoid interactive 
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update

# Python 3.9
RUN apt-get update
RUN apt-get install -y python3.9 python3-pip

# Java 1.8
RUN apt-get install -y openjdk-8-jdk

# PySpark
RUN pip3 install pyspark==3.5.0 numpy pandas

#ADD

#CMD

