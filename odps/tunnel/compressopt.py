#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from enum import Enum


class CompressOption(object):

    class CompressAlgorithm(Enum):
        ODPS_RAW = 'RAW'
        ODPS_ZLIB = 'ZLIB'
        ODPS_SNAPPY = 'SNAPPY'

    def __init__(self, compress_algo=CompressAlgorithm.ODPS_ZLIB,
                 level=None, strategy=None):
        if isinstance(compress_algo, CompressOption.CompressAlgorithm):
            self.algorithm = compress_algo
        else:
            self.algorithm = \
                CompressOption.CompressAlgorithm(compress_algo.upper())
        self.level = level or 1
        self.strategy = strategy or 0
