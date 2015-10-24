#!/usr/bin/env bash
export PYTHONPATH=$H2O_HOME/h2o-py:$SPARKLING_HOME/py:$PYTHONPATH
export SPARK_CLASSPATH=$SPARK_CLASSPATH:$SPARKLING_HOME/assembly/build/libs/sparkling-water-assembly-0.2.17-SNAPSHOT-all.jar
echo $SPARK_CLASSPATH

IPYTHON_OPTS="notebook --config=~/.ipython/profile_pyspark/ipython_notebook_config.py" $SPARK_HOME/bin/pyspark

