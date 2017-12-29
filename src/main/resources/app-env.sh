#!/usr/bin/env bash

SPARK_HOME="/opt/spark-2.2.0-bin-hadoop2.6/"
SPARK_MASTER="yarn"
MAIN_CLASS="correlator.main.CorrelatorLaunch"
#MAIN_CLASS="com.fosun.fonova.bdt.data.correlator.main.CorrelatedDataSelector"
SPARK_SUBMIT_OPTS="--principal ranmx/fonova-hadoop0.fx01@FONOVA.COM  --keytab /home/ranmx/ranmx-hadoop0.fx01.keytab --queue root.dev --deploy-mode client --driver-memory 8g --executor-cores 4 --executor-memory 30g --num-executors 20"
#HADOOP_CONF_DIR=
#JAVA_HOME=
#CLASS_PATH=
#JAVA_OPTS=
