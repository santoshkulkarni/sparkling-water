# encoding: utf-8
# module pySparkling
# from (pysparkling)
"""
pySparkling - The Sparkling-Water Python Package
=====================
"""
__version__ = "SUBST_PROJECT_VERSION"

# set imports from this project which will be available when the module is imported
from .dataframe import DataFrame
from .connection import SparklingWaterConnection
from .context import H2OContext

# set what is meant by * packages in statement from foo import *
#__all__ = []
