#include <string>

#include "google/protobuf/stubs/common.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

using namespace google::protobuf;

namespace pbfast {

class ProtoWriter 
{
public:
	ProtoWriter();
	~ProtoWriter();

	void WriteLengthDelimitedTag(int field_num);
	void WriteUInt32(int field_num, uint32 value);

	void WriteSInt64(int field_num, int64 value); 
	void WriteBool(int field_num, bool value);
	void WriteDouble(int field_num, double value);
	void WriteString(int field_num, char* value, uint32 length);

	void WriteVarint32(uint32 value);
	void WriteStringNoTag(char* value, uint32 length);
	void WriteSInt64NoTag(int64 value);
	void WriteBoolNoTag(bool value);
	void WriteDoubleNoTag(double value);
	void WriteBoolNoTagRaw(bool value);

	void EndRecord();
	void Complete();
	void Reset();
	int64 ByteCount() const;
	std::string ByteString(void);

private:
	std::string *mString;
	io::StringOutputStream *mStringOutput;
    io::CodedOutputStream *mCodedOutput;

    uint32 mRecordCrc;
    uint32 mCrcCrc;
    int64 mRecordCount;
};

}