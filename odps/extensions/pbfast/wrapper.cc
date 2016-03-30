// -*- C++ -*-
#include <Python.h>
#include <string>
#include <sstream>
#include <iostream>

#include "structmember.h"
#include "proto_writer.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace {
    extern PyTypeObject ProtoWriterType;
    
    typedef struct {
      PyObject_HEAD

      pbfast::ProtoWriter *writer;
    } ProtoWriter;

    void
    ProtoWriter_dealloc(ProtoWriter* self)
    {
        delete self->writer;
        self->ob_type->tp_free((PyObject*)self);
    }

    PyObject *
    ProtoWriter_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
    {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        ProtoWriter *self;
        self = (ProtoWriter *)type->tp_alloc(type, 0);
        self->writer = new pbfast::ProtoWriter();

        return (PyObject *)self;
    }

    static PyObject *
    ProtoWriter_repr(PyObject *selfObject)
    {
        ProtoWriter *self = (ProtoWriter *)selfObject;
        std::stringstream result;
        result << "ProtoWriter: ";
        result << self->writer;
        std::string resultString = result.str();
        return PyUnicode_Decode(resultString.data(), resultString.length(), "utf-8", NULL);
    }

    PyObject *
    ProtoWriter_WriteSInt64(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int index;
        ::google::protobuf::int64 value;

        if (!PyArg_ParseTuple(args, "iL", &index, &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteSInt64(index, value);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteLengthDelimitedTag(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int index;

        if (!PyArg_ParseTuple(args, "i", &index)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteLengthDelimitedTag(index);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteUInt32(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int index;
        ::google::protobuf::uint32 value;

        if (!PyArg_ParseTuple(args, "iI", &index, &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteUInt32(index, value);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }


    PyObject *
    ProtoWriter_WriteVarint32(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        ::google::protobuf::uint32 value;

        if (!PyArg_ParseTuple(args, "I", &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteVarint32(value);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteSInt64NoTag(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        ::google::protobuf::int64 value;

        if (!PyArg_ParseTuple(args, "L", &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteSInt64NoTag(value);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }


    PyObject *
    ProtoWriter_WriteBool(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int index;
        int value;

        if (!PyArg_ParseTuple(args, "ii", &index, &value)) {
            return 0;
        }



        Py_BEGIN_ALLOW_THREADS
        if (value == 0) {
            self->writer->WriteBool(index, false);
        } else {
            self->writer->WriteBool(index, true);
        }
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteBoolNoTag(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int value;

        if (!PyArg_ParseTuple(args, "i", &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        if (value == 0) {
            self->writer->WriteBoolNoTag(false);
        } else {
            self->writer->WriteBoolNoTag(true);
        }
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteBoolNoTagRaw(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int value;

        if (!PyArg_ParseTuple(args, "i", &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        if (value == 0) {
            self->writer->WriteBoolNoTagRaw(false);
        } else {
            self->writer->WriteBoolNoTagRaw(true);
        }
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteDouble(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int index;
        double value;

        if (!PyArg_ParseTuple(args, "id", &index, &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteDouble(index, value);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteDoubleNoTag(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        double value;

        if (!PyArg_ParseTuple(args, "d", &value)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteDoubleNoTag(value);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteString(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        int index;
        char* value;
        ::google::protobuf::uint32 length;

        if (!PyArg_ParseTuple(args, "is#", &index, &value, &length)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteString(index, value, length);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_WriteStringNoTag(ProtoWriter *self, PyObject *args) {
        if (args == NULL || args == Py_None) {
            return 0;
        }

        char* value;
        ::google::protobuf::uint32 length;

        if (!PyArg_ParseTuple(args, "s#", &value, &length)) {
            return 0;
        }

        Py_BEGIN_ALLOW_THREADS
        self->writer->WriteStringNoTag(value, length);
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_ByteString(ProtoWriter *self, PyObject *nothing) {
        std::string result;
        Py_BEGIN_ALLOW_THREADS
        result = self->writer->ByteString();
        Py_END_ALLOW_THREADS
        return PyString_FromStringAndSize(result.data(), result.length());
    }

    PyObject *
    ProtoWriter_ByteCount(ProtoWriter *self, PyObject *nothing) {
        int64 result;

        Py_BEGIN_ALLOW_THREADS
        result = self->writer->ByteCount();
        Py_END_ALLOW_THREADS        
        return PyLong_FromLongLong(result);
    }

    PyObject *
    ProtoWriter_Reset(ProtoWriter *self, PyObject *nothing) {
        Py_BEGIN_ALLOW_THREADS
        self->writer->Reset();
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_EndRecord(ProtoWriter *self, PyObject *nothing) {
        Py_BEGIN_ALLOW_THREADS
        self->writer->EndRecord();
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    PyObject *
    ProtoWriter_Complete(ProtoWriter *self, PyObject *nothing) {
        Py_BEGIN_ALLOW_THREADS
        self->writer->Complete();
        Py_END_ALLOW_THREADS
        Py_RETURN_NONE;
    }

    int
    ProtoWriter_init(ProtoWriter *self, PyObject *args, PyObject *kwds)
    {
        return 0;
    }

    PyMemberDef ProtoWriter_members[] = {
        {NULL}  // Sentinel
    };

    PyGetSetDef ProtoWriter_getsetters[] = {
        {NULL}  // Sentinel
    };

    PyMethodDef ProtoWriter_methods[] = {
        {"WriteLengthDelimitedTag", (PyCFunction)ProtoWriter_WriteLengthDelimitedTag, METH_VARARGS,
         "Write a Length Delimited Tag to buffer"
        },
        {"WriteUInt32", (PyCFunction)ProtoWriter_WriteUInt32, METH_VARARGS,
         "Write a UInt32 to buffer"
        },
        {"WriteSInt64", (PyCFunction)ProtoWriter_WriteSInt64, METH_VARARGS,
         "Write a SInt64 to buffer"
        },
        {"WriteBool", (PyCFunction)ProtoWriter_WriteBool, METH_VARARGS,
         "Write a Bool to buffer"
        },
        {"WriteDouble", (PyCFunction)ProtoWriter_WriteDouble, METH_VARARGS,
         "Write a Double to buffer"
        },
        {"WriteVarint32", (PyCFunction)ProtoWriter_WriteVarint32, METH_VARARGS,
         "Write a Varint32 to buffer"
        },
        {"WriteSInt64NoTag", (PyCFunction)ProtoWriter_WriteSInt64NoTag, METH_VARARGS,
         "Write a SInt64 to buffer"
        },
        {"WriteBoolNoTag", (PyCFunction)ProtoWriter_WriteBoolNoTag, METH_VARARGS,
         "Write a Bool to buffer"
        },
        {"WriteBoolNoTagRaw", (PyCFunction)ProtoWriter_WriteBoolNoTagRaw, METH_VARARGS,
         "Write a Bool to buffer"
        },
        {"WriteDoubleNoTag", (PyCFunction)ProtoWriter_WriteDoubleNoTag, METH_VARARGS,
         "Write a Double to buffer"
        },
        {"WriteString", (PyCFunction)ProtoWriter_WriteString, METH_VARARGS,
         "Write a String to buffer"
        },
        {"WriteStringNoTag", (PyCFunction)ProtoWriter_WriteStringNoTag, METH_VARARGS,
         "Write a String to buffer"
        },
        {"ByteString", (PyCFunction)ProtoWriter_ByteString, METH_NOARGS,
         "Buffer String of the output buffer"
        },
        {"Reset", (PyCFunction)ProtoWriter_Reset, METH_NOARGS,
         "Reset"
        },
        {"EndRecord", (PyCFunction)ProtoWriter_EndRecord, METH_NOARGS,
         "End Record"
        },
        {"Complete", (PyCFunction)ProtoWriter_Complete, METH_NOARGS,
         "Complete all"
        },
        {"ByteCount", (PyCFunction)ProtoWriter_ByteCount, METH_NOARGS,
         "ByteCount"
        },
        {NULL}  // Sentinel
    };

    PyTypeObject ProtoWriterType = {
        PyObject_HEAD_INIT(NULL)
        0,                                      /*ob_size*/
        "pbfast.ProtoWriter",  /*tp_name*/
        sizeof(ProtoWriter),             /*tp_basicsize*/
        0,                                      /*tp_itemsize*/
        (destructor)ProtoWriter_dealloc, /*tp_dealloc*/
        0,                                      /*tp_print*/
        0,                                      /*tp_getattr*/
        0,                                      /*tp_setattr*/
        0,                                      /*tp_compare*/
        ProtoWriter_repr,                /*tp_repr*/
        0,                                      /*tp_as_number*/
        0,                                      /*tp_as_sequence*/
        0,                                      /*tp_as_mapping*/
        0,                                      /*tp_hash */
        0,                                      /*tp_call*/
        0,                                      /*tp_str*/
        0,                                      /*tp_getattro*/
        0,                                      /*tp_setattro*/
        0,                                      /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
        "ProtoWriter objects",           /* tp_doc */
        0,                                      /* tp_traverse */
        0,                                      /* tp_clear */
        0,         /* tp_richcompare */
        0,                                                /* tp_weaklistoffset */
        0,                                                /* tp_iter */
        0,                                                  /* tp_iternext */
        ProtoWriter_methods,             /* tp_methods */
        ProtoWriter_members,             /* tp_members */
        ProtoWriter_getsetters,          /* tp_getset */
        0,                                      /* tp_base */
        0,                                      /* tp_dict */
        0,                                      /* tp_descr_get */
        0,                                      /* tp_descr_set */
        0,                                      /* tp_dictoffset */
        (initproc)ProtoWriter_init,      /* tp_init */
        0,                                      /* tp_alloc */
        ProtoWriter_new,                 /* tp_new */
    };
} // namespace


static PyMethodDef module_methods[] = {
    {NULL}  // Sentinel
};


#ifndef PyMODINIT_FUNC  // Declarations for DLL import/export.
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initpbfast(void)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    PyObject* m;




      if (PyType_Ready(&ProtoWriterType) < 0)
          return;


    m = Py_InitModule3("pbfast", module_methods,
                       "");

    if (m == NULL)
      return;




      Py_INCREF(&ProtoWriterType);
      PyModule_AddObject(m, "ProtoWriter", (PyObject *)&ProtoWriterType);

}