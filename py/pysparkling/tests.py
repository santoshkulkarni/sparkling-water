#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for PySparkling; 
"""

import sys
import os

import unittest2 as unittest

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.tests import ReusedPySparkTestCase

from pysparkling.context import H2OContext

# We need spark home for testing
SPARK_HOME = os.environ["SPARK_HOME"]

class ReusedPySparklingTestCase(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        super(ReusedPySparklingTestCase, cls).setUpClass()
        cls.hc = H2OContext(cls.sc)

class H2OContextTestCase(ReusedPySparklingTestCase):

    def test_as_h2o_frame(self):
        print("AA")
        pass



