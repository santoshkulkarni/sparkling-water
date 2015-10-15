# pySparkling

## Goal
Provide transparent user experience of using Sparkling Water from Python.
It includes:
  - support creation of H2OContext
  - support data transfers - from H2OFrame to DataFrame/RDD and back

## Usage

Command to launch pyspark with Sparkling Water:
 ```
PYSPARK_PYTHON=ipython bin/pyspark --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2
```

Command to launch Python script `script.py` via `spark-submit`:
```
bin/spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 script.py
```

Creating H2O Context:
```
hc = H2OContext(sc)
```

## Technical details

Use Py4J bundled with PySpark to access JVM classes

```
from py4j.java_gateway import java_import
java_import(sc._jvm, "org.apache.spark.h2o.*")
```

## Problems

Spark Issues
  - https://issues.apache.org/jira/browse/SPARK-5185 - `--jars` packages are not appended to driver path
     * Solution: `sc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("com.cloudera.science.throwaway.ThrowAway").newInstance()`
     * ```
       We've been setting SPARK_SUBMIT_CLASSPATH as a workaround to this issue, but as of https://github.com/apache/spark/commit/517975d89d40a77c7186f488547eed11f79c1e97 this variable no longer exists. We're now setting SPARK_CLASSPATH as a workaround.
       ```

  Related issue:
    - https://issues.apache.org/jira/browse/SPARK-6047


 # Neat
   - https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/DynamicClassLoader.java
