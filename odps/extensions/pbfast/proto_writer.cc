#include <iostream>

#include "google/protobuf/stubs/common.h"
#include "google/protobuf/wire_format_lite.h"
#include "google/protobuf/wire_format_lite_inl.h"

#include "proto_writer.h"
#include "crc32c.c"

#define META_TOTAL_RECORDS_TAG 33554430
#define META_CHECKSUM_TAG 33554431
#define RECORD_END_TAG 33553408

#define CRC_INIT 0


using namespace google::protobuf::internal;
using namespace pbfast;


void Dump( const void * mem, unsigned int n ) {
  const char * p = reinterpret_cast< const char *>( mem );
  for ( unsigned int i = 0; i < n; i++ ) {
     std::cout << hex << int(p[i]) << " ";
  }
  std::cout << std::endl;
}

/**
 * 使用一个定长的 byte array 存放 protobuf stream 编码出来的二进制
 */
ProtoWriter::ProtoWriter() {
    mString = new std::string();
	mStringOutput = new io::StringOutputStream(mString);
	mCodedOutput = new io::CodedOutputStream(mStringOutput);
	mRecordCount = 0;
	mRecordCrc = CRC_INIT;
	mCrcCrc = CRC_INIT;
}

ProtoWriter::~ProtoWriter() {
	delete mCodedOutput;
	delete mStringOutput;
	delete mString;
}

std::string ProtoWriter::ByteString(void) {
	// c_str has no \0
	return std::string(mString->c_str(), ByteCount());
}

int64 ProtoWriter::ByteCount(void) const {
	return mCodedOutput->ByteCount();
}

void ProtoWriter::Reset(void) {
	delete mCodedOutput;
	delete mStringOutput;
	delete mString;
    mString = new std::string();
	mStringOutput = new io::StringOutputStream(mString);
	mCodedOutput = new io::CodedOutputStream(mStringOutput);
}

void ProtoWriter::WriteLengthDelimitedTag(int field_num) {
	WireFormatLite::WriteTag(field_num, WireFormatLite::WIRETYPE_LENGTH_DELIMITED, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &field_num, sizeof(int));
	// std::cout << "checksum: " << mRecordCrc << std::endl;
}

void ProtoWriter::WriteSInt64(int field_num, int64 value) {
	WireFormatLite::WriteSInt64(field_num, value, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &field_num, sizeof(int));
	mRecordCrc = crc32c(mRecordCrc, &value, sizeof(int64));
}

void ProtoWriter::WriteBool(int field_num, bool value) {
    WireFormatLite::WriteBool(field_num, value, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &field_num, sizeof(int));
	mRecordCrc = crc32c(mRecordCrc, &value, sizeof(bool));
}

void ProtoWriter::WriteDouble(int field_num, double value) {
    WireFormatLite::WriteDouble(field_num, value, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &field_num, sizeof(int));
	mRecordCrc = crc32c(mRecordCrc, &value, sizeof(double));
}

void ProtoWriter::WriteString(int field_num, char* value, uint32 length) {
	WriteLengthDelimitedTag(field_num);
    mCodedOutput->WriteVarint32(length); // length
    mCodedOutput->WriteRaw(value, length);
	mRecordCrc = crc32c(mRecordCrc, value, length);
	// std::cout << "checksum: " << mRecordCrc << std::endl;

}

void ProtoWriter::WriteVarint32(uint32 value) {
    mCodedOutput->WriteVarint32(value);
}

void ProtoWriter::WriteSInt64NoTag(int64 value) {
	WireFormatLite::WriteSInt64NoTag(value, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &value, sizeof(int64));
}

void ProtoWriter::WriteBoolNoTag(bool value) {
    WireFormatLite::WriteBoolNoTag(value, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &value, sizeof(bool));
}

void ProtoWriter::WriteBoolNoTagRaw(bool value) {
    WireFormatLite::WriteBoolNoTag(value, mCodedOutput);
}

void ProtoWriter::WriteDoubleNoTag(double value) {
    WireFormatLite::WriteDoubleNoTag(value, mCodedOutput);
	mRecordCrc = crc32c(mRecordCrc, &value, sizeof(double));
}

void ProtoWriter::WriteStringNoTag(char* value, uint32 length) {
    mCodedOutput->WriteVarint32(length); // length
    mCodedOutput->WriteRaw(value, length);
    mRecordCrc = crc32c(mRecordCrc, value, length);
}

void ProtoWriter::EndRecord() {
	WriteUInt32(RECORD_END_TAG, mRecordCrc);
	mCrcCrc = crc32c(mCrcCrc, &mRecordCrc, sizeof(uint32));
	mRecordCrc = CRC_INIT;
	mRecordCount++;
}

void ProtoWriter::Complete() {
	WriteSInt64(META_TOTAL_RECORDS_TAG, mRecordCount);
	WriteUInt32(META_CHECKSUM_TAG, mCrcCrc);
	mRecordCount = 0;
	mRecordCrc = CRC_INIT;
	mCrcCrc = CRC_INIT;
}

void ProtoWriter::WriteUInt32(int field_num, uint32 value) {
	WireFormatLite::WriteUInt32(field_num, value, mCodedOutput);
}



