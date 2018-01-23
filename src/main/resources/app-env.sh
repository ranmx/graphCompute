#!/usr/bin/env bash

SPARK_HOME="/opt/spark-2.2.0-bin-hadoop2.6/"
SPARK_MASTER="yarn"
MAIN_CLASS="correlator.main.FidMapping"
#MAIN_CLASS="com.fosun.fonova.bdt.data.correlator.main.CorrelatedDataSelector"
SPARK_SUBMIT_OPTS="--principal fosundb/fonova-app02@FONOVA_AHZ.COM  --keytab /data/fosundb/fosundb_fonova-app02@FONOVA_AHZ.COM.keytab --queue root.ahz_batch.dev --deploy-mode cluster --driver-memory 4g --executor-cores 1 --executor-memory 2g --num-executors 25"
#HADOOP_CONF_DIR=
#JAVA_HOME=
#CLASS_PATH=
#JAVA_OPTS=

