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

import math
from datetime import datetime
from decimal import Decimal
import random
import time
from multiprocessing.pool import ThreadPool
try:
    from string import letters
except ImportError:
    from string import ascii_letters as letters

from odps.tests.core import TestBase, to_str
from odps.compat import unittest, OrderedDict
from odps.models import Schema
from odps import types
from odps.errors import ODPSError


class Test(TestBase):
    def _upload_data(self, test_table, records, compress=False, **kw):
        upload_ss = self.tunnel.create_upload_session(test_table, **kw)
        writer = upload_ss.open_bufferred_writer(compress=compress)
        for r in records:
            record = upload_ss.new_record()
            for i, it in enumerate(r):
                record[i] = it
            writer.write(record)
        writer.close()
        upload_ss.commit()

    def _upload_data_as_tuples(self, test_table, records, compress=False, **kw):
        upload_ss = self.tunnel.create_upload_session(test_table, **kw)
        writer = upload_ss.open_bufferred_writer(compress=compress)
        for r in records:
            writer.write(r)
        writer.close()
        upload_ss.commit()

    def _download_data(self, test_table, compress=False, columns=None, **kw):
        download_ss = self.tunnel.create_download_session(test_table, **kw)
        with download_ss.open_record_reader(0, 3, compress=compress, columns=columns) as reader:
            records = [tuple(record.values) for record in reader]
            return records

    def _gen_data(self):
        return [
            ('hello world', 2**63-1, math.pi, datetime(2015, 9, 19, 2, 11, 25, 33000),
             True, Decimal('3.14'), ['simple', 'easy'], OrderedDict({'s': 1})),
            ('goodbye', 222222, math.e, datetime(2020, 3, 10), False, Decimal('2.555555'),
             ['true', None], OrderedDict({'true': 1})),
            ('c'*300, -2**63+1, -2.222, datetime(1999, 5, 25, 3, 10), True, Decimal(22222),
             ['false'], OrderedDict({'false': 0})),
        ]

    def _create_table(self, table_name):
        fields = ['id', 'int_num', 'float_num', 'dt', 'bool', 'dec', 'arr', 'm']
        types = ['string', 'bigint', 'double', 'datetime', 'boolean', 'decimal',
                 'array<string>', 'map<string,bigint>']

        self.odps.delete_table(table_name, if_exists=True)
        return self.odps.create_table(table_name, schema=Schema.from_lists(fields, types))

    def _create_partitioned_table(self, table_name):
        fields = ['id', 'int_num', 'float_num', 'dt', 'bool', 'dec', 'arr', 'm']
        types = ['string', 'bigint', 'double', 'datetime', 'boolean', 'decimal',
                 'array<string>', 'map<string,bigint>']

        self.odps.delete_table(table_name, if_exists=True)
        return self.odps.create_table(table_name,
                                      schema=Schema.from_lists(fields, types, ['ds'], ['string']))

    def _delete_table(self, table_name):
        self.odps.delete_table(table_name)

    def testUploadAndDownloadByRawTunnel_AsTuple(self):
        test_table_name = 'pyodps_test_raw_tunnel'
        self._create_table(test_table_name)
        data = self._gen_data()
        self._upload_data_as_tuples(test_table_name, data)
        records = self._download_data(test_table_name)

        self.assertSequenceEqual(data, records)
        self._delete_table(test_table_name)

    def testUploadByRawTunnel_AsList_NegCases(self):
        test_table_name = 'pyodps_test_raw_tunnel'
        self._create_table(test_table_name)
        # wrong type
        with self.assertRaises(TypeError):
            r = [0, None, None, None, None, None, None, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(TypeError):
            r = [None, "fake_type_bigint", None, None, None, None, None, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(TypeError):
            r = [None, None, "fake_type_double", None, None, None, None, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(AttributeError):
            r = [None, None, None, 0, None, None, None, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(TypeError):
            r = [None, None, None, None, "fake_type_boolean", None, None, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(ODPSError):
            r = [None, None, None, None, None, "not_a_decimal", None, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(TypeError):
            r = [None, None, None, None, None, None, 0, None]
            self._upload_data_as_tuples(test_table_name, [r])
        with self.assertRaises(AttributeError):
            r = [None, None, None, None, None, None, None, "not_a_map"]
            self._upload_data_as_tuples(test_table_name, [r])

        # wrong value
        with self.assertRaises(OverflowError):
            r = [None, 2**63, None, None, None, None, None, None]
            self._upload_data_as_tuples(test_table_name, [r])

    def testUploadAndDownloadByRawTunnel(self):
        test_table_name = 'pyodps_test_raw_tunnel'
        self._create_table(test_table_name)
        data = self._gen_data()
        self._upload_data(test_table_name, data)
        records = self._download_data(test_table_name)

        self.assertSequenceEqual(data, records)
        self._delete_table(test_table_name)

    def testDownloadWithSpecifiedColumns(self):
        test_table_name = 'pyodps_test_raw_tunnel_columns'
        self._create_table(test_table_name)

        data = self._gen_data()
        self._upload_data(test_table_name, data)

        records = self._download_data(test_table_name, columns=['id'])
        self.assertSequenceEqual([r[0] for r in records], [r[0] for r in data])
        for r in records:
            for i in range(1, len(r)):
                self.assertIsNone(r[i])
        self._delete_table(test_table_name)

    def testPartitionUploadAndDownloadByRawTunnel_AsTuples(self):
        test_table_name = 'pyodps_test_raw_partition_tunnel'
        test_table_partition = 'ds=test'
        self.odps.delete_table(test_table_name, if_exists=True)

        table = self._create_partitioned_table(test_table_name)
        table.create_partition(test_table_partition)
        data = self._gen_data()

        self._upload_data_as_tuples(test_table_name, data, partition_spec=test_table_partition)
        records = self._download_data(test_table_name, partition_spec=test_table_partition)
        self.assertSequenceEqual(data, [r[:-1] for r in records])

        self._delete_table(test_table_name)

    def testPartitionUploadAndDownloadByRawTunnel(self):
        test_table_name = 'pyodps_test_raw_partition_tunnel'
        test_table_partition = 'ds=test'
        self.odps.delete_table(test_table_name, if_exists=True)

        table = self._create_partitioned_table(test_table_name)
        table.create_partition(test_table_partition)
        data = self._gen_data()

        self._upload_data(test_table_name, data, partition_spec=test_table_partition)
        records = self._download_data(test_table_name, partition_spec=test_table_partition)
        self.assertSequenceEqual(data, [r[:-1] for r in records])

        self._delete_table(test_table_name)

    def testPartitionDownloadWithSpecifiedColumns(self):
        test_table_name = 'pyodps_test_raw_tunnel_partition_columns'
        test_table_partition = 'ds=test'
        self.odps.delete_table(test_table_name, if_exists=True)

        table = self._create_partitioned_table(test_table_name)
        table.create_partition(test_table_partition)
        data = self._gen_data()

        self._upload_data(test_table_name, data, partition_spec=test_table_partition)
        records = self._download_data(test_table_name, partition_spec=test_table_partition,
                                      columns=['int_num'])
        self.assertSequenceEqual([r[1] for r in data], [r[0] for r in records])

        self._delete_table(test_table_name)

    def testUploadAndDownloadByZlibTunnel(self):
        test_table_name = 'pyodps_test_zlib_tunnel'
        self._create_table(test_table_name)
        data = self._gen_data()

        self._upload_data(test_table_name, data, compress=True)
        records = self._download_data(test_table_name, compress=True)
        self.assertSequenceEqual(data, records)

        self._delete_table(test_table_name)

    def testUploadAndDownloadBySnappyTunnel(self):
        test_table_name = 'pyodps_test_snappy_tunnel'
        self._create_table(test_table_name)
        data = self._gen_data()

        self._upload_data(test_table_name, data, compress=True, compress_algo='snappy')
        records = self._download_data(test_table_name, compress=True, compress_algo='snappy')
        self.assertSequenceEqual(data, records)

        self._delete_table(test_table_name)

    def _gen_random_bigint(self):
        return random.randint(*types.bigint._bounds)

    def _gen_random_string(self, max_length=15):
        gen_letter = lambda: letters[random.randint(0, 51)]
        return to_str(''.join([gen_letter() for _ in range(random.randint(1, 15))]))

    def _gen_random_double(self):
        return random.uniform(-2**32, 2**32)

    def _gen_random_datetime(self):
        return datetime.fromtimestamp(random.randint(0, int(time.time())))

    def _gen_random_boolean(self):
        return random.uniform(-1, 1) > 0

    def _gen_random_decimal(self):
        return Decimal(str(self._gen_random_double()))

    def _gen_random_array_type(self):
        t = random.choice(['string', 'bigint', 'double', 'boolean'])
        return types.Array(t)

    def _gen_random_map_type(self):
        random_key_type = random.choice(['bigint', 'string'])
        random_value_type = random.choice(['bigint', 'string', 'double'])

        return types.Map(random_key_type, random_value_type)

    def _gen_random_array(self, random_type, size=None):
        size = size or random.randint(100, 500)

        random_type = types.validate_data_type(random_type)
        if isinstance(random_type, types.Array):
            random_type = random_type.value_type
        method = getattr(self, '_gen_random_%s' % random_type.name)
        array = [method() for _ in range(size)]

        return array

    def _gen_random_map(self, random_map_type):
        size = random.randint(100, 500)

        random_map_type = types.validate_data_type(random_map_type)

        key_arrays = self._gen_random_array(random_map_type.key_type, size)
        value_arrays = self._gen_random_array(random_map_type.value_type, size)

        m = OrderedDict(zip(key_arrays, value_arrays))
        return m

    def _gen_table(self, partition=None, partition_type=None, partition_val=None, size=100):
        def gen_name(name):
            if '<' in name:
                name = name.split('<', 1)[0]
            if len(name) > 4:
                name = name[:4]
            else:
                name = name[:2]
            return name

        test_table_name = 'pyodps_test_tunnel'
        types = ['bigint', 'string', 'double', 'datetime', 'boolean', 'decimal']
        types.append(self._gen_random_array_type().name)
        types.append(self._gen_random_map_type().name)
        random.shuffle(types)
        names = [gen_name(t) for t in types]

        self.odps.delete_table(test_table_name, if_exists=True)
        partition_names = [partition, ] if partition else None
        partition_types = [partition_type, ] if partition_type else None
        table = self.odps.create_table(
            test_table_name,
            Schema.from_lists(names, types, partition_names=partition_names,
                              partition_types=partition_types))
        if partition_val:
            table.create_partition('%s=%s' % (partition, partition_val))

        data = []
        for _ in range(size):
            record = []
            for t in types:
                n = t.split('<', 1)[0]
                method = getattr(self, '_gen_random_'+n)
                if n in ('map', 'array'):
                    record.append(method(t))
                else:
                    record.append(method())
            data.append(record)

        return table, data

    def testTableUploadAndDownloadTunnel(self):
        table, data = self._gen_table()

        records = [table.new_record(values=d) for d in data]
        self.odps.write_table(table, 0, records)

        reads = list(self.odps.read_table(table, len(data)))
        for val1, val2 in zip(data, [r.values for r in reads]):
            for it1, it2 in zip(val1, val2):
                if isinstance(it1, dict):
                    self.assertEqual(len(it1), len(it2))
                    self.assertTrue(any(it1[k] == it2[k] for k in it1))
                elif isinstance(it1, list):
                    self.assertSequenceEqual(it1, it2)
                else:
                    self.assertEqual(it1, it2)

        table.drop()

    def testMultiTableUploadAndDownloadTunnel(self):
        table, data = self._gen_table(size=10)

        records = [table.new_record(values=d) for d in data]

        self.odps.write_table(table, 0, records[:5])
        self.odps.write_table(table, 0, records[5:])

        reads = list(self.odps.read_table(table, len(data)))
        for val1, val2 in zip(data, [r.values for r in reads]):
            for it1, it2 in zip(val1, val2):
                if isinstance(it1, dict):
                    self.assertEqual(len(it1), len(it2))
                    self.assertTrue(any(it1[k] == it2[k] for k in it1))
                elif isinstance(it1, list):
                    self.assertSequenceEqual(it1, it2)
                else:
                    self.assertEqual(it1, it2)

    def testParallelTableUploadAndDownloadTunnel(self):
        p = 'ds=test'

        table, data = self._gen_table(partition=p.split('=', 1)[0], partition_type='string',
                                      partition_val=p.split('=', 1)[1])
        self.assertTrue(table.exist_partition(p))
        records = [table.new_record(values=d) for d in data]

        n_blocks = 5
        blocks = list(range(n_blocks))
        n_threads = 2
        thread_pool = ThreadPool(n_threads)

        def gen_block_records(block_id):
            c = len(data)
            st = int(c / n_blocks * block_id)
            if block_id < n_blocks - 1:
                ed = int(c / n_blocks * (block_id + 1))
            else:
                ed = c
            return records[st: ed]

        def write(w):
            def inner(arg):
                idx, r = arg
                w.write(idx, r)
            return inner

        with table.open_writer(partition=p, blocks=blocks) as writer:
            thread_pool.map(write(writer), [(i, gen_block_records(i)) for i in blocks])

        for step in range(1, 4):
            reads = []
            expected = []

            with table.open_reader(partition=p) as reader:
                count = reader.count

                for i in range(n_blocks):
                    start = int(count / n_blocks * i)
                    if i < n_blocks - 1:
                        end = int(count / n_blocks * (i + 1))
                    else:
                        end = count
                    for record in reader[start:end:step]:
                        reads.append(record)
                    expected.extend(data[start:end:step])

            self.assertEqual(len(expected), len(reads))
            for val1, val2 in zip(expected, [r.values for r in reads]):
                for it1, it2 in zip(val1[:-1], val2[:-1]):
                    if isinstance(it1, dict):
                        self.assertEqual(len(it1), len(it2))
                        self.assertTrue(any(it1[k] == it2[k] for k in it1))
                    elif isinstance(it1, list):
                        self.assertSequenceEqual(it1, it2)
                    else:
                        self.assertEqual(it1, it2)

        table.drop()

if __name__ == '__main__':
    unittest.main()
