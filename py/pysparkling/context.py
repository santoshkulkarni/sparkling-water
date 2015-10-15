from pyspark.context import SparkContext
import h2o

class H2OContext(object):

    def __init__(self, sparkContext):
        try:
            self._do_init(sparkContext)
        except:
            raise

    def _do_init(self, sparkContext):
        self._sc = sparkContext
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

    def start(self, init_h2o_client = True):
        self._jhc.start()
        self._client_ip = self._jhc.h2oLocalClientIp()
        self._client_port = self._jhc.h2oLocalClientPort()
        if (init_h2o_client) {
        }
        

    def stop(self):
        self._jhc.stop(False)

    def __str__(self):
        return "H2OContext ip={}, port={}".format(self._client_ip, self._client_port)

    def get_client(self):
        """
        Returns h2o client.
        """
        pass



