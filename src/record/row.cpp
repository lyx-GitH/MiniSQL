#include "record/row.h"

#define setTrue(bytes, bit) bytes |= (uint64_t(1) << (bit) )
#define getBit(bytes, bit) (((bytes) >> (bit)) & 1)

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  uint32_t field_num = schema->GetColumnCount();
  uint32_t ser_cnt = 0;

  ASSERT(field_num <= 64, "Row::SerializeTo : Schema Too Long");
  ASSERT(schema->GetColumnCount() == GetFieldCount(), "Row::SerializeTo : Schema Not Match");

  if(field_num == 0) {
    //empty fields, write Nothing
    return 0;
  }

  uint64_t NullMap = 0;



  MACH_WRITE_UINT32(buf, field_num);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  //skip null map
  char* null_buf_begin = buf;
  MOVE_FORWARD(buf, ser_cnt, uint64_t);

  auto columns = schema->GetColumns();

  for(uint32_t i=0; i<fields_.size(); i++) {
    if(fields_[i]->IsNull()) {
      ASSERT(columns[i]->IsNullable(), "Row::SerializeTo : Null Value Against Non-null Column");
      continue;
    }

    setTrue(NullMap, i);
    fields_[i]->SerializeTo(buf);
    STEP_FORWARD(buf, ser_cnt, fields_[i]->GetSerializedSize());
  }

  //write NullMap
  MACH_WRITE_UINT64(null_buf_begin, NullMap);

  return ser_cnt;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  if(schema->GetColumnCount() == 0)
    return 0;

  uint32_t ser_cnt = 0;
  uint32_t schema_size;
  uint64_t NullMap;

  //read size
  schema_size = MACH_READ_UINT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  ASSERT(schema_size == schema->GetColumnCount(), "Row::DeserializeFrom : Schema Size Not Match");
  ASSERT(schema_size <= 64, "Row::DeserializeFrom : Schema Size Too Large");
  ASSERT(heap_, "Row::DeserializeFrom : Null Heap");

  fields_.resize(schema_size);

  //read NullMap
  NullMap = MACH_READ_UINT64(buf);
  MOVE_FORWARD(buf, ser_cnt, uint64_t);

  TypeId type;
  uint32_t steps;


  for(uint32_t i=0; i<schema_size; i++) {
    Field* fd;
    type = schema->GetColumn(i)->GetType();

    if(getBit(NullMap, i) == 0 ) {
      //Null Field
      Field::DeserializeFrom(buf, type, &fd, true, heap_);
    } else {
      //Not Null Field
      steps = Field::DeserializeFrom(buf, type, &fd, false, heap_);
      STEP_FORWARD(buf, ser_cnt, steps);
    }

    fields_[i] = fd;
  }

  return ser_cnt;

}

uint32_t Row::GetSerializedSize(Schema *schema) const {
if(fields_.empty())
  return 0;

//header:
auto ser_cnt = sizeof (uint32_t) + sizeof (uint64_t);

for(auto field : fields_) {
  ser_cnt += field->GetSerializedSize();
}

return ser_cnt;

}
