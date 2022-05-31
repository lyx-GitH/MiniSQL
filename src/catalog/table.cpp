#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t ser_cnt = 0;

  // write magic number
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // write table id
  MACH_WRITE_UINT32(buf, table_id_);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // write table name
  MACH_WRITE_UINT32(buf, table_name_.length());
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  MACH_WRITE_STRING(buf, table_name_);
  STEP_FORWARD(buf, ser_cnt, table_name_.length());

  // write root page id
  MACH_WRITE_UINT32(buf, root_page_id_);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // write schema
//  STEP_FORWARD(buf, ser_cnt, schema_->SerializeTo(buf));
  auto steps = schema_->SerializeTo(buf);
  STEP_FORWARD(buf, ser_cnt, steps);

  return ser_cnt;
}

uint32_t TableMetadata::GetSerializedSize() const {
  /*
  ints: TABLE_METADATA_MAGIC_NUM, table_id_, table_name_.length(), root_page_id_
  Schema: schema_
  */
  return sizeof(uint32_t) * 4 + schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  ASSERT(table_meta == nullptr, "TableMetadata::DeserializeFrom : Not Null TableMetadata");

  uint32_t magic_number;
  uint32_t table_id;
  uint32_t table_name_len;
  std::string table_name;
  uint32_t root_page_id;

  uint32_t ser_cnt = 0;
  uint32_t i;

  // read and check magic_number
  magic_number = MACH_READ_UINT32(buf);
  ASSERT(magic_number == TABLE_METADATA_MAGIC_NUM, "TableMetadata::DeserializeFrom : Magic Number Not Match");
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read table id
  table_id = MACH_READ_UINT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read table name
  table_name_len = MACH_READ_UINT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  for (i = 0; i < table_name_len; ++i)
  {
    table_name.append(1, buf[0]);
    ++buf;
    ++ser_cnt;
  }

  // read root page id
  root_page_id = MACH_READ_UINT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read schema
  Schema *schema = nullptr;
  uint32_t step = Schema::DeserializeFrom(buf, schema, heap);
  STEP_FORWARD(buf, ser_cnt, step);

  table_meta = Create(table_id, table_name, root_page_id, schema, heap);

  return ser_cnt;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));

  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}