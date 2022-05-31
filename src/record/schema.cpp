#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ser_cnt = 0;

  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  MACH_WRITE_UINT32(buf, columns_.size());
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  uint32_t steps = 0;

  for (auto column : columns_) {
    steps = column->SerializeTo(buf);
    ASSERT(steps = column->GetSerializedSize(), "Invalid Ser");
    STEP_FORWARD(buf, ser_cnt, steps);
  }
  //LOG(INFO)<<GetSerializedSize();
  //ASSERT(ser_cnt == GetSerializedSize(), "Schema::SerializeTo : MisAligned ser_cnt");
  return ser_cnt;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  /*
   * uint32_t:
   * magic_number;
   * length of columns_
   * -----------------------
   * Columns:
   * for_each column in columns_
   */

  if (columns_.empty()) {
    return 2 * sizeof(uint32_t);
  } else {
//    auto c_size = columns_[0]->GetSerializedSize();
    uint32_t c_size = 0;
    for(auto& col : columns_)
      c_size += col->GetSerializedSize();
    return 2 * sizeof(uint32_t) + c_size ;
  }
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  ASSERT(schema == nullptr, "Schema::DeserializeFrom : Not Null Schema");
  uint32_t magic_number;
  uint32_t columns_length;

  uint32_t ser_cnt = 0;
  uint32_t steps = 0;

  // read and check magic_number
  magic_number = MACH_READ_UINT32(buf);
  ASSERT(magic_number == SCHEMA_MAGIC_NUM, "Schema::DeserializeFrom : Magic Number Not Match");
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read col_size
  columns_length = MACH_READ_UINT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  void *mem = heap->Allocate(sizeof(Schema));

  if (columns_length == 0) {
    schema = new (mem) Schema({});
  } else {
    auto vec = std::vector<Column *>(columns_length);
    // retrieve all columns
    for (uint32_t i = 0; i < columns_length; i++) {
      Column *col = nullptr;
      steps = Column::DeserializeFrom(buf, col, heap);
      STEP_FORWARD(buf, ser_cnt, steps);
      vec[i] = col;
    }
    schema = new (mem) Schema(vec);
  }

  //ASSERT(ser_cnt == schema->GetSerializedSize(), "Schema::DeserializeFrom : MisAligned ser_cnt");
  return ser_cnt;
}