from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import SQLContext
from types import *

try:
    import h2o
    from h2o.frame import H2OFrame
    has_h2o = True
except Exception:
    println("H2O package is not available!")
    has_h2o = False

def _monkey_patch_H2OFrame():
    @staticmethod
    def determine_java_vec_type(vec):
        if vec.isEnum():
            return "enum"
        elif vec.isUUID():
            return "uuid"
        elif vec.isString():
            return "string"
        elif vec.isInt():
            if vec.isTime():
                return "time"
            else:
                return "int"
        else:
            return "real"

    @staticmethod
    def from_java_h2o_frame(h2o_frame, h2o_frame_id):
        fr = H2OFrame()
        fr._backed_by_java_obj = True
        fr._nrows = h2o_frame.numRows()
        fr._ncols = h2o_frame.numCols()
        fr._id = h2o_frame_id.toString()
        fr._computed = True
        fr._keep = True
        fr._col_names =  [c for c in h2o_frame.names()]
        types = [H2OFrame.determine_java_vec_type(h2o_frame.vec(name)) for name in fr._col_names]
        fr._types = dict(zip(fr._col_names,types))
        return fr
    H2OFrame.determine_java_vec_type = determine_java_vec_type
    H2OFrame.from_java_h2o_frame = from_java_h2o_frame

class H2OContext(object):

    def __init__(self, sparkContext):
        try:
            # Hack H2OFrame from h2o package
            _monkey_patch_H2OFrame()
            self._do_init(sparkContext)
        except:
            raise

    def _do_init(self, sparkContext):
        self._sc = sparkContext
        # do not instantiate sqlContext when already one exists
        jsqlContext = self._sc._jvm.SQLContext.getOrCreate(self._sc._jsc.sc())
        self._sqlContext = SQLContext(sparkContext,jsqlContext)
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
        """
        Start H2OContext.

        It initializes H2O services on each node in Spark cluster and creates
        H2O python client.
        """
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

    def as_data_frame(self, h2o_frame):
        if isinstance(h2o_frame,H2OFrame):
            jdf = self._jhc.asDataFrame(h2o_frame, self._sqlContext)
            return DataFrame(jdf,self._sqlContext)

    def is_of_simple_type(self, rdd):
        if not isinstance(rdd, RDD):
            raise ValueError('rdd is not of type pyspark.rdd.RDD')

        if isinstance(rdd.first(), (StringType, IntType, FloatType)):
            return True
        else:
            return False

    def get_first(self, rdd):
        if rdd.isEmpty():
            raise ValueError('rdd is empty')

        return rdd.first()

    def as_h2o_frame(self, dataframe):
        if isinstance(dataframe, DataFrame):
            j_h2o_frame = self._jhc.asH2OFrame(dataframe._jdf)
            j_h2o_frame_key = self._jhc.toH2OFrameKey(dataframe._jdf)
            return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)
        elif isinstance(dataframe, RDD):
            # First check if the type T in RDD[T] is primitive - String, Float, Int
            if self.is_of_simple_type(dataframe):
                first = self.get_first(dataframe)
                if isinstance(first,StringType):
                    j_h2o_frame = self._jhc.asH2OFrameFromRDDString(dataframe._to_java_object_rdd())
                    j_h2o_frame_key = self._jhc.asH2OFrameFromRDDStringKey(dataframe._to_java_object_rdd())
                    return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)
                elif isinstance(first, IntType):
                    j_h2o_frame =  self._jhc.asH2OFrameFromRDDInt(dataframe._to_java_object_rdd())
                    j_h2o_frame_key = self._jhc.asH2OFrameFromRDDIntKey(dataframe._to_java_object_rdd())
                    return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)
                elif isinstance(first, FloatType):
                    j_h2o_frame = self._jhc.asH2OFrameFromRDDDouble(dataframe._to_java_object_rdd())
                    j_h2o_frame_key = self._jhc.asH2OFrameFromRDDDoubleKey(dataframe._to_java_object_rdd())
                    return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)
            else:
                # Creates a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.
                # On scala backend, to transform RDD of Product to H2OFrame, we need to know Type Tag.
                # Since there is no alternative for Product class in Python, we first transform the rdd to dataframe
                # and then transform it to H2OFrame.
                df = self._sqlContext.createDataFrame(dataframe)
                j_h2o_frame = self._jhc.asH2OFrame(df._jdf)
                j_h2o_frame_key = self._jhc.toH2OFrameKey(df._jdf)
                return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)
