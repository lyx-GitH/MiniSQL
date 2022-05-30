#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  ASSERT(buf != nullptr, "Column::SerializeTo: Passing Null Buf");
  uint32_t ser_size = 0;

  // write magic number
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write name_
  MACH_WRITE_UINT32(buf, name_.length());
  MOVE_FORWARD(buf, ser_size, uint32_t);

  MACH_WRITE_STRING(buf, name_);
  STEP_FORWARD(buf, ser_size, name_.length());

  // write type
  MACH_WRITE_UINT32(buf, type_);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write len
  MACH_WRITE_UINT32(buf, len_);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write table_ind_
  MACH_WRITE_UINT32(buf, table_ind_);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write nullable and unique
  *buf = static_cast<char>(nullable_);
  MOVE_FORWARD(buf, ser_size, char);

  *buf = static_cast<char>(unique_);
  MOVE_FORWARD(buf, ser_size, char);

  return ser_size;
}

uint32_t Column::GetSerializedSize() const {
  /*
   * ints:
   * 1. type
   * 2. name.length
   * 3. table_ind_
   * 4. magic_number
   * 5. len_
   * ------------
   * chars:
   * 1. nullable
   * 2. unique_
   * ------------
   * strings:
   * name_
   */
  return 5 * sizeof(uint32_t) + 2 * sizeof(char) + name_.length();
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  ASSERT(buf != nullptr, "Column::DeserializeFrom : Null buf");
  ASSERT(column == nullptr, "Column::DeserializeFrom: Not Null column");

  uint32_t ser_cnt = 0;
  uint32_t i;

  bool nullable, unique;
  uint32_t len, table_ind, name_len, magic_number;
  TypeId type;

  // read and check magic number
  magic_number = MACH_READ_INT32(buf);
  ASSERT(magic_number == COLUMN_MAGIC_NUM, "Column::DeserializeFrom : Magic Number Unmatched");
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read string
  name_len = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);
  std::string name;

  for (i = 0; i < name_len; i++) {
    name.append(1, buf[0]);
    ++buf;
    ++ser_cnt;
  }

  // read type
  type = TypeId(MACH_READ_INT32(buf));
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read len
  len = MACH_READ_UINT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read table ind
  table_ind = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  nullable = *buf;
  MOVE_FORWARD(buf, ser_cnt, char);

  unique = *buf;
  MOVE_FORWARD(buf, ser_cnt, char);

  void *mem = heap->Allocate(sizeof(Column));

  if (type == TypeId::kTypeChar) {
    column = new (mem) Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new (mem) Column(name, type, table_ind, nullable, unique);
  }

  return ser_cnt;
}
