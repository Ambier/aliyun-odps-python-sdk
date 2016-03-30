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

import zlib
import six
import logging
import time
from .. import utils, types, compat
from odps.tunnel.compressopt import CompressOption
from odps.tunnel.pbfast import ProtoWriter

LOG = logging.getLogger(__name__)


class BufferedWriter(object):

    def __init__(self, schema, upload_block, buffer_size=10*1024*1024, compress_option=None,
                 compress_algo=None, compres_level=None, compress_strategy=None,
                 encoding='utf-8'):
        self._buffer_size = buffer_size
        self._writer = ProtoWriter()
        self._encoding = encoding
        self._curr_cursor = 0
        self._n_bytes = 0
        self._schema = schema
        self._upload_block = upload_block
        self._columns = self._schema.columns
        self._block_id = 0
        self._compress_option = compress_option
        self._start_time = time.time()

        if compress_option is None and compress_algo is not None:
            self._compress_option = CompressOption(compress_algo=compress_algo, level=compres_level,
                                                   strategy=compress_strategy)
        self._init_compressor()

    def _init_compressor(self):
        if self._compress_option is None:
            self._compressor = None
        elif self._compress_option.algorithm == CompressOption.CompressAlgorithm.ODPS_RAW:
            self._compressor = None
        elif self._compress_option.algorithm == CompressOption.CompressAlgorithm.ODPS_ZLIB:
            self._compressor = zlib.compressobj(self._compress_option.level, zlib.DEFLATED, zlib.MAX_WBITS,
                                                zlib.DEF_MEM_LEVEL, self._compress_option.strategy)
        else:
            raise IOError('Invalid compression option.')

    def write(self, record):
        n_record_fields = len(record)
        n_columns = len(self._columns)

        if n_record_fields > n_columns:
            raise IOError('record fields count is more than schema.')

        for i in range(min(n_record_fields, n_columns)):
            val = record[i]
            if val is None:
                continue

            pb_index = i + 1
            data_type = self._columns[i].type

            if isinstance(data_type, types.Boolean):
                self._writer.WriteBool(pb_index, val)

            elif isinstance(data_type, types.Datetime):
                val = utils.to_milliseconds(val)
                self._writer.WriteSInt64(pb_index, val)

            elif isinstance(data_type, types.String):
                if isinstance(val, six.text_type):
                    val = val.encode(self._encoding)
                self._writer.WriteString(pb_index, val)

            elif isinstance(data_type, types.Double):
                self._writer.WriteDouble(pb_index, val)

            elif isinstance(data_type, types.Bigint):
                self._writer.WriteSInt64(pb_index, val)

            elif isinstance(data_type, types.Decimal):
                val = str(val)
                if isinstance(val, six.text_type):
                    val = val.encode(self._encoding)
                self._writer.WriteString(pb_index, val)

            elif isinstance(data_type, types.Array):
                self._writer.WriteLengthDelimitedTag(pb_index)
                self._write_array(val, data_type.value_type)

            elif isinstance(data_type, types.Map):
                self._writer.WriteLengthDelimitedTag(pb_index)
                self._write_map(val, data_type.key_type, data_type.value_type)
            else:
                raise IOError('Invalid data type: %s' % data_type)

        self._writer.EndRecord()
        self._curr_cursor += 1
        # Flush the buffer if full
        if self._writer.ByteCount() >= self._buffer_size:
            self.flush()

    def flush(self):
        LOG.info("BufferredWriter flushes block#" + str(self._block_id) + " size: " + str(self._writer.ByteCount()))
        self._writer.Complete()
        self._n_bytes += self._writer.ByteCount()
        raw_data = self._writer.ByteString()
        self._writer.Reset()

        def gen_data():
            if self._compressor:
                compressed = self._compressor.compress(raw_data)
                remaining = self._compressor.flush()
                self._init_compressor()
                yield compressed + remaining
            else:
                yield raw_data

        self._upload_block(self._block_id, gen_data())
        self._block_id += 1

    def close(self):
        if self._writer.ByteCount() > 0:
            self.flush()
        LOG.info("BufferredWriter: %f MiB/s blocks: %d" % (float(self.n_bytes / 1024 / 1024) / (time.time() - self._start_time), self._block_id))

    def _write_array(self, data, data_type):
        self._writer.WriteVarint32(len(data))
        for value in data:
            if value is None:
                self._writer.WriteBoolNoTagRaw(True)
            else:
                self._writer.WriteBoolNoTagRaw(False)
                self._write_primitive(value, data_type)

    def _write_map(self, data, key_type, value_type):
        self._write_array(compat.lkeys(data), key_type)
        self._write_array(compat.lvalues(data), value_type)

    def _write_primitive(self, data, data_type):
        if data_type == types.string:
            if isinstance(data, six.text_type):
                data = data.encode(self._encoding)
            self._writer.WriteStringNoTag(data)
        elif data_type == types.bigint:
            self._writer.WriteSInt64NoTag(data)
        elif data_type == types.double:
            self._writer.WriteDoubleNoTag(data)
        elif data_type == types.boolean:
            self._writer.WriteBoolNoTag(data)
        else:
            raise IOError('Not a primitive type in array. type: %s' % data_type)

    @property
    def count(self):
        return self._curr_cursor

    @property
    def n_bytes(self):
        return self._n_bytes

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

