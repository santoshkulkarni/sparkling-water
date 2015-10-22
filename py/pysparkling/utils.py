from h2o.frame import H2OFrame

class FrameConversions:
    
    @staticmethod
    def _as_h2o_frame_from_RDD_String(h2oContext, rdd):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDString(rdd._to_java_object_rdd())
        j_h2o_frame_key = h2oContext._jhc.asH2OFrameFromRDDStringKey(rdd._to_java_object_rdd())
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_RDD_Bool(h2oContext, rdd):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDBool(rdd._to_java_object_rdd())
        j_h2o_frame_key = h2oContext._jhc.asH2OFrameFromRDDBoolKey(rdd._to_java_object_rdd())
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_RDD_Int(h2oContext, rdd):
        return FrameConversions._as_h2o_frame_from_RDD_Long(h2oContext,rdd)

    @staticmethod
    def _as_h2o_frame_from_RDD_Double(h2oContext, rdd):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDDouble(rdd._to_java_object_rdd())
        j_h2o_frame_key = h2oContext._jhc.asH2OFrameFromRDDDoubleKey(rdd._to_java_object_rdd())
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_RDD_Float(h2oContext, rdd):
        return FrameConversions._as_h2o_frame_from_RDD_Double(h2oContext,rdd)

    @staticmethod
    def _as_h2o_frame_from_RDD_Long(h2oContext, rdd):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDLong(rdd._to_java_object_rdd())
        j_h2o_frame_key = h2oContext._jhc.asH2OFrameFromRDDLongKey(rdd._to_java_object_rdd())
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_dataframe(h2oContext, dataframe):
        j_h2o_frame = h2oContext._jhc.asH2OFrame(dataframe._jdf)
        j_h2o_frame_key = h2oContext._jhc.toH2OFrameKey(dataframe._jdf)
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_complex_type(h2oContext,dataframe):
        # Creates a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.
        # On scala backend, to transform RDD of Product to H2OFrame, we need to know Type Tag.
        # Since there is no alternative for Product class in Python, we first transform the rdd to dataframe
        # and then transform it to H2OFrame.
        df = h2oContext._sqlContext.createDataFrame(dataframe)
        j_h2o_frame = h2oContext._jhc.asH2OFrame(df._jdf)
        j_h2o_frame_key = h2oContext._jhc.toH2OFrameKey(df._jdf)
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)