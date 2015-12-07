#!/bin/bash
#SPARK_HOME=
JARS=`ls -dm ./lib/*.jar | tr -d ' '`
SUBMIT=$SPARK_HOME'/bin/spark-submit'
TARGET=./target/scala-2.10/telecomanalytics_2.10-1.1.jar
CLASS=$1
SMASTER=$2
#PROPERTIES=$SPARK_HOME'/conf/spark-defaults.conf'
rm -rf ../../work/*
#$SUBMIT --verbose --jars $JARS --class $CLASS --master $SMASTER --properties-file $PROPERTIES $TARGET ${@:2}
$SUBMIT --verbose --jars $JARS --class $CLASS --master $SMASTER $TARGET ${@:2}
