#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map, MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new (buf) IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t ser_size = 0;
  uint32_t i;

  // write magic number
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write index id
  MACH_WRITE_UINT32(buf, index_id_);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write index name
  MACH_WRITE_UINT32(buf, index_name_.length());
  MOVE_FORWARD(buf, ser_size, uint32_t);

  MACH_WRITE_STRING(buf, index_name_);
  STEP_FORWARD(buf, ser_size, index_name_.length());

  // write table id
  MACH_WRITE_UINT32(buf, table_id_);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  //write root_page_id
  MACH_WRITE_INT32(buf, root_page_id_);
  MOVE_FORWARD(buf, ser_size, uint32_t);


  // write key map
  MACH_WRITE_UINT32(buf, key_map_.size());
  MOVE_FORWARD(buf, ser_size, uint32_t);

  for (i = 0; i < key_map_.size(); ++i) {
    MACH_WRITE_UINT32(buf, key_map_[i]);
    MOVE_FORWARD(buf, ser_size, uint32_t);
  }

  return ser_size;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  /*
  ints: INDEX_METADATA_MAGIC_NUM, index_id_, table_id_, index_name_.length(), key_map_.size(), key_map_
  string: index_name_
  */
  return sizeof(uint32_t) * (6 + key_map_.size()) + index_name_.size();
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  ASSERT(buf != nullptr, "IndexMetadata::DeserializeFrom : Null buf");
  ASSERT(index_meta == nullptr, "IndexMetadata::DeserializeFrom: Not Null index_meta");

  uint32_t ser_cnt = 0, magic_number, index_name_len, key_map_len;
  uint32_t i;

  index_id_t index_id;
  std::string index_name;
  table_id_t table_id;
  uint32_t key_id;
  page_id_t root_page_id;
  std::vector<uint32_t> key_map;

  // read and check magic number
  magic_number = MACH_READ_INT32(buf);
  ASSERT(magic_number == INDEX_METADATA_MAGIC_NUM, "IndexMetadata::DeserializeFrom : Magic Number Unmatched");
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read index id
  index_id = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read index name
  index_name_len = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  for (i = 0; i < index_name_len; ++i) {
    index_name.append(1, buf[0]);
    ++buf;
    ++ser_cnt;
  }

  // read table id
  table_id = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  //read root_id
  root_page_id = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read key map
  key_map_len = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  for (i = 0; i < key_map_len; ++i) {
    key_id = MACH_READ_INT32(buf);
    MOVE_FORWARD(buf, ser_cnt, uint32_t);
    key_map.push_back(key_id);
  }

  index_meta = Create(index_id, index_name, table_id, key_map, heap);
  index_meta->root_page_id_ = root_page_id;

  return ser_cnt;
}