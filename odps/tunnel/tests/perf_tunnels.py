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

import time
import cProfile

from odps.compat import unittest
from odps.tests.core import TestBase
from pstats import Stats
from odps.models import Schema


class Test(TestBase):
    OPTIMIZE_PB = True
    COMPRESS_DATA = False
    BUFFER_SIZE = 1024*1024*10
    DATA_AMOUNT = 50*10000
    STRING_LITERAL = "Soft kitty, warm kitty, little ball of fur; happy kitty, sleepy kitty, purr, purr"

    def setUp(self):
        TestBase.setUp(self)
        self.pr = cProfile.Profile()
        self.pr.enable()

    def tearDown(self):
        p = Stats(self.pr)
        p.strip_dirs()
        p.sort_stats('cumtime')
        p.print_stats(40)
        p.print_callees('writer.py:123\(write', 10)
        p.print_callees('writerpbfast.py:77\(write', 30)
        TestBase.teardown(self)

    def testWrite(self):
        from datetime import datetime
        from decimal import Decimal
        table_name = 'test_tunnel_write_correctly'
        fields = ['a', 'b', 'c', 'd', 'e', 'f']
        types = ['bigint', 'double', 'datetime', 'boolean', 'string', 'decimal']
        self.odps.create_table(table_name, schema=Schema.from_lists(fields, types), if_not_exists=True)
        ss = self.tunnel.create_upload_session(table_name)
        r = ss.new_record()

        start = time.time()
        with ss.open_record_writer(0, optimize_pb=self.OPTIMIZE_PB) as writer:
            for i in range(self.DATA_AMOUNT):
                r[0] = 2**63-1
                r[1] = 0.0001
                r[2] = datetime(2015, 11, 11)
                r[3] = True
                r[4] = self.STRING_LITERAL
                r[5] = Decimal('3.15')
                writer.write(r)
        print float(writer.n_bytes / 1024 / 1024) / (time.time() - start), 'MiB/s'

        ss.commit([0])
        self.odps.delete_table(table_name)

    def testWrite_AsTuples(self):
        from datetime import datetime
        from decimal import Decimal
        table_name = 'test_tunnel_write_correctly'
        fields = ['a', 'b', 'c', 'd', 'e', 'f']
        types = ['bigint', 'double', 'datetime', 'boolean', 'string', 'decimal']
        self.odps.create_table(table_name, schema=Schema.from_lists(fields, types), if_not_exists=True)
        ss = self.tunnel.create_upload_session(table_name)

        start = time.time()
        with ss.open_record_writer(0, optimize_pb=self.OPTIMIZE_PB) as writer:
            for i in range(self.DATA_AMOUNT):
                r = [2**63-1, 0.0001, datetime(2015, 11, 11), True, self.STRING_LITERAL, Decimal('3.15')]
                writer.write(r)

        print float(writer.n_bytes / 1024 / 1024) / (time.time() - start), 'MiB/s'
        ss.commit([0])
        self.odps.delete_table(table_name)

    def testWrite_BufferredWriter(self):
        from datetime import datetime
        from decimal import Decimal
        table_name = 'test_tunnel_write_correctly'
        fields = ['a', 'b', 'c', 'd', 'e', 'f']
        types = ['bigint', 'double', 'datetime', 'boolean', 'string', 'decimal']
        self.odps.create_table(table_name, schema=Schema.from_lists(fields, types), if_not_exists=True)
        ss = self.tunnel.create_upload_session(table_name)
        r = ss.new_record()

        with ss.open_bufferred_writer(buffer_size=self.BUFFER_SIZE, compress=self.COMPRESS_DATA) as writer:
            for i in range(self.DATA_AMOUNT):
                r[0] = 2**63-1
                r[1] = 0.0001
                r[2] = datetime(2015, 11, 11)
                r[3] = True
                r[4] = self.STRING_LITERAL
                r[5] = Decimal('3.15')
                writer.write(r)

        ss.commit()
        with self.odps.get_table(table_name).open_reader() as reader:
            print reader.count
        self.odps.delete_table(table_name)

    def testWrite_AsTuples_BufferredWriter(self):
        from datetime import datetime
        from decimal import Decimal
        table_name = 'test_tunnel_write_correctly'
        fields = ['a', 'b', 'c', 'd', 'e', 'f']
        types = ['bigint', 'double', 'datetime', 'boolean', 'string', 'decimal']
        self.odps.create_table(table_name, schema=Schema.from_lists(fields, types), if_not_exists=True)
        ss = self.tunnel.create_upload_session(table_name)

        with ss.open_bufferred_writer(buffer_size=self.BUFFER_SIZE, compress=self.COMPRESS_DATA) as writer:
            for i in range(self.DATA_AMOUNT):
                r = [2**63-1, 0.0001, datetime(2015, 11, 11), True, self.STRING_LITERAL, Decimal('3.15')]
                writer.write(r)
        ss.commit()
        self.odps.delete_table(table_name)


if __name__ == '__main__':
    unittest.main()
