#!/usr/bin/env python

import unittest
import pbfast as pb


def display(w, raw=True):
  if raw:
    print str(w.ByteCount()) + ": " + repr(w.ByteString())
  else:
    print str(w.ByteCount()) + ": " + w.ByteString()

def sep(s):
  print "*"*40 + s + "*"*40

class Test_proto_wrapper(unittest.TestCase):
  
  def testInit(self):
    w = pb.ProtoWriter()
    print repr(w)

  def testWritePrimitives(self):
    w = pb.ProtoWriter()
    print repr(w)

    for i in range(4):
      s = "abcdefg"+str(i)
      w.WriteString(0, s)
      w.WriteBool(1, False)      
      w.WriteDouble(2, 123.123456)
      w.WriteSInt64(3, 123456+i)
      w.WriteUInt32(4, 123456+i)
      display(w)
      w.Reset()
    display(w)

  def testChecksum(self):
    import crc32c
    import struct
    checksum = crc32c.Crc32c()

    print checksum.getvalue()

    val = bytearray(struct.pack('<i', 1))
    print repr(val)
    checksum.update(val, 0, len(val))
    print checksum.getvalue()

    val = bytearray('hello')
    print repr(val)
    checksum.update(val, 0, len(val))
    print checksum.getvalue()

    w = pb.ProtoWriter()
    w.WriteString(1, 'hello')
    display(w)


  def testWritePrimitivesNoTag(self):
    w = pb.ProtoWriter()
    print repr(w)

    for i in range(10):
      w.WriteStringNoTag("abcdefg"+str(i))
      w.WriteBoolNoTag(True)
      w.WriteDoubleNoTag(123.123456)
      w.WriteSInt64NoTag(123456+i)
      w.WriteVarint32(123456+i)
    display(w)

  def testReset(self):
    w = pb.ProtoWriter()
    print repr(w)

    for i in range(10):
      w.WriteStringNoTag("abcdefg"+str(i))
      display(w)
      if w.ByteCount() >= 25:
        sep("reset")
        w.Reset()
    sep("remaining")
    display(w)


def suite():
  suite = unittest.TestSuite()
  
  suite.addTests(unittest.makeSuite(Test_proto_wrapper))
  
  return suite
