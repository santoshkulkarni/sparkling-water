from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import SQLContext
from types import *

try:
    import h2o
    has_h2o = True
except Exception:
    println("H2O package is not available!")
    has_h2o = False

class H2OContext(object):

    def __init__(self, sparkContext):
        try:
            self._do_init(sparkContext)
        except:
            raise

    def _do_init(self, sparkContext):
        self._sc = sparkContext
        self._sqlContext = SQLContext(sparkContext)
        self._jsc = sparkContext._jsc
        self._jvm = sparkContext._jvm
        self._gw = sparkContext._gateway

        # Imports Sparkling Water into current JVM view
        # We cannot use directly Py4j to import Sparkling Water packages
        #   java_import(sc._jvm, "org.apache.spark.h2o.*")
        # because of https://issues.apache.org/jira/browse/SPARK-5185
        # So lets load class directly via classloader
        jvm = self._jvm
        sc = self._sc
        gw = self._gw
        # Load class
        jhc_klazz = jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("org.apache.spark.h2o.H2OContext")
        # Find ctor with right spark context
        jctor_def = gw.new_array(jvm.Class, 1)
        jctor_def[0] = sc._jsc.getClass()
        jhc_ctor = jhc_klazz.getConstructor(jctor_def)
        jctor_params = gw.new_array(jvm.Object, 1)
        jctor_params[0] = sc._jsc
        # Create instance of class
        jhc = jhc_ctor.newInstance(jctor_params)
        self._jhc = jhc
        self._client_ip = None
        self._client_port = None

    def start(self, init_h2o_client = True, strict_version_check = False):
        self._jhc.start()
        self._client_ip = self._jhc.h2oLocalClientIp()
        self._client_port = self._jhc.h2oLocalClientPort()

        if (has_h2o):
            if (init_h2o_client):
                h2o.init(ip=self._client_ip, port=self._client_port, strict_version_check = strict_version_check)
        else:
            println("H2O package is not available!")

    def stop(self):
        self._jhc.stop(False)

    def __str__(self):
        return "H2OContext ip={}, port={}".format(self._client_ip, self._client_port)


    def as_h2o_frame(self, dataframe):
        if isinstance(dataframe, DataFrame):
            jdf = dataframe._jdf
            return self._jhc.asH2OFrame(dataframe._jdf)
        elif isinstance(dataframe, RDD):
            # First check if the type T in RDD[T] is primitive - String, Float Int
            if isinstance(dataframe.first(), StringType):
                return self._jhc.asH2OFrameFromRDDString(dataframe._to_java_object_rdd())
            elif isinstance(dataframe.first(), IntType):
                return self._jhc.asH2OFrameFromRDDInt(dataframe._to_java_object_rdd())
            elif isinstance(dataframe.first(), FloatType):
                return self._jhc.asH2OFrameFromRDDDouble(dataframe._to_java_object_rdd())
            else:
                # Creates a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.
                # On scala backend, to transform RDD of Product to H2OFrame, we need to know Type Tag.
                # Since there is no alternative for Product class in Python, we first transform the rdd to dataframe
                # and then transform it to H2OFrame.
                df = self._sqlContext.createDataFrame(dataframe)
                return self._jhc.asH2OFrame(df._jdf)

