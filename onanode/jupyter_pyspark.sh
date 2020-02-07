#!/bin/bash

sudo easy_install pip
sudo pip install jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --port=8085 --ip=0.0.0.0'
export PYSPARK_DRIVER_PYTHON=jupyter

sudo gsutil cp gs://etsy-mlinfra-prod-shared-user-scratch-data-6bsh/jars/hadoop-lzo-0.4.20.jar /usr/lib/spark/jars
nohup pyspark --jars /usr/lib/spark/jars/hadoop-lzo-0.4.20.jar &
tail nohup.out

printf "http://%s:%s\n" $(hostname -I) $(cat nohup.out | grep -o '8085.*' | tail -n1)