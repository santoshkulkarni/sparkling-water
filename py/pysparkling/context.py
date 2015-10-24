from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import SQLContext
from h2o.frame import H2OFrame
from pysparkling.utils import FrameConversions as fc

try:
    import h2o
    from h2o.frame import H2OFrame
    has_h2o = True
except Exception:
    println("H2O package is not available!")
    has_h2o = False

def _monkey_patch_H2OFrame(hc):
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


    def get_java_h2o_frame(self):
        if hasattr(self, '_java_frame'):
            return self._java_frame
        else:
            return hc._jhc.asH2OFrame(self._get()._id)

    @staticmethod
    def from_java_h2o_frame(h2o_frame, h2o_frame_id):
        fr = H2OFrame()
        fr._java_frame = h2o_frame
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
    H2OFrame.get_java_h2o_frame = get_java_h2o_frame

class H2OContext(object):

    def __init__(self, sparkContext):
        try:
            self._do_init(sparkContext)
            # Hack H2OFrame from h2o package
            _monkey_patch_H2OFrame(self)
        except:
            raise

    def _do_init(self, sparkContext):
        self._sc = sparkContext
        # do not instantiate sqlContext when already one exists
        self._jsqlContext = self._sc._jvm.SQLContext.getOrCreate(self._sc._jsc.sc())
        self._sqlContext = SQLContext(sparkContext,self._jsqlContext)
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
            return self
        else:
            println("H2O package is not available!")
            return None

    def stop(self):
        self._jhc.stop(False)

    def __str__(self):
        return "H2OContext ip={}, port={}".format(self._client_ip, self._client_port)

    def as_data_frame(self, h2o_frame):
        if isinstance(h2o_frame,H2OFrame):
            j_h2o_frame = h2o_frame.get_java_h2o_frame()
            jdf = self._jhc.asDataFrame(j_h2o_frame, self._jsqlContext)
            return DataFrame(jdf,self._sqlContext)

    def is_of_simple_type(self, rdd):
        if not isinstance(rdd, RDD):
            raise ValueError('rdd is not of type pyspark.rdd.RDD')

        if isinstance(rdd.first(), (str, int, bool, long, float)):
            return True
        else:
            return False

    def get_first(self, rdd):
        if rdd.isEmpty():
            raise ValueError('rdd is empty')

        return rdd.first()

    def as_h2o_frame(self, dataframe):
        if isinstance(dataframe, DataFrame):
            return fc._as_h2o_frame_from_dataframe(self,dataframe)
        elif isinstance(dataframe, RDD):
            # First check if the type T in RDD[T] is one of the python "primitive" types
            # String, Boolean, Int and Double (Python Long is converted to java.lang.BigInteger)
            if self.is_of_simple_type(dataframe):
                first = self.get_first(dataframe)
                if isinstance(first, str):
                    return fc._as_h2o_frame_from_RDD_String(self,dataframe)
                elif isinstance(first, bool):
                    return fc._as_h2o_frame_from_RDD_Bool(self,dataframe)
                elif isinstance(dataframe.max(), int):
                    return fc._as_h2o_frame_from_RDD_Long(self,dataframe)
                elif isinstance(first, float):
                    return fc._as_h2o_frame_from_RDD_Float(self,dataframe)
                elif isinstance(dataframe.max(), long):
                    raise ValueError('Numbers in RDD Too Big')
            else:
                return fc._as_h2o_frame_from_complex_type(self,dataframe)
