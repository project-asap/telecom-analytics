#!/bin/bash
SPARK_HOME='/home/forth/Spark-Nested/'
SUBMIT=$SPARK_HOME'bin/spark-submit'
SMASTER="spark://sparkMaster@locahost:7077"
TARGET=./target/scala-2.10/PeakDetection-assembly-1.1.jar
CLASS=$1
$SUBMIT --verbose --jars ./lib/spark-assembly-1.1.0-hadoop1.0.4.jar --class $CLASS --master $SMASTER $TARGET ${@:2}
