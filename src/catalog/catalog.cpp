#include "catalog/catalog.h"
#include "common/macros.h"

CatalogMeta::CatalogMeta(std::map<table_id_t, page_id_t> table_meta_pages_,
                         std::map<index_id_t, page_id_t> index_meta_pages_) {
  this->table_meta_pages_ = table_meta_pages_;
  this->index_meta_pages_ = index_meta_pages_;
}
void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");

  uint32_t table_meta_pages_num = table_meta_pages_.size();
  uint32_t index_meta_pages_num = index_meta_pages_.size();
  uint32_t ser_size = 0;

  ser_size++;
  ser_size--;

  // write magic number
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  // write table meta pages
  MACH_WRITE_UINT32(buf, table_meta_pages_num);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  auto it = table_meta_pages_.begin();
  for (; it != table_meta_pages_.end(); ++it) {
    MACH_WRITE_UINT32(buf, it->first);
    MOVE_FORWARD(buf, ser_size, uint32_t);

    MACH_WRITE_UINT32(buf, it->second);
    MOVE_FORWARD(buf, ser_size, uint32_t);
  }

  // write index meta pages
  MACH_WRITE_UINT32(buf, index_meta_pages_num);
  MOVE_FORWARD(buf, ser_size, uint32_t);

  it = index_meta_pages_.begin();
  for (; it != index_meta_pages_.end(); ++it) {
    MACH_WRITE_UINT32(buf, it->first);
    MOVE_FORWARD(buf, ser_size, uint32_t);

    MACH_WRITE_UINT32(buf, it->second);
    MOVE_FORWARD(buf, ser_size, uint32_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  ASSERT(buf != nullptr, "*CatalogMeta::DeserializeFrom : Null buf");

  uint32_t ser_cnt = 0;
  uint32_t i;
  uint32_t magic_number = 0, table_meta_pages_num, index_meta_pages_num;
  uint32_t table_id, page_id, index_id;

  // to silence warning
  ser_cnt++;
  ser_cnt--;

  magic_number++;

  std::map<table_id_t, page_id_t> table_meta_pages;
  std::map<index_id_t, page_id_t> index_meta_pages;

  // read and check magic number
  magic_number = MACH_READ_UINT32(buf);
  ASSERT(magic_number == CATALOG_METADATA_MAGIC_NUM, "*CatalogMeta::DeserializeFrom : Magic Number Unmatched");
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  // read table meta pages and sorting into a map
  table_meta_pages_num = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  for (i = 0; i < table_meta_pages_num; ++i) {
    table_id = MACH_READ_INT32(buf);
    MOVE_FORWARD(buf, ser_cnt, uint32_t);

    page_id = MACH_READ_INT32(buf);
    MOVE_FORWARD(buf, ser_cnt, uint32_t);

    table_meta_pages.insert(pair<table_id_t, page_id_t>(table_id, page_id));
  }

  // read table meta pages and sorting into a map
  index_meta_pages_num = MACH_READ_INT32(buf);
  MOVE_FORWARD(buf, ser_cnt, uint32_t);

  for (i = 0; i < index_meta_pages_num; ++i) {
    index_id = MACH_READ_INT32(buf);
    MOVE_FORWARD(buf, ser_cnt, uint32_t);

    page_id = MACH_READ_INT32(buf);
    MOVE_FORWARD(buf, ser_cnt, uint32_t);

    index_meta_pages.insert(pair<index_id_t, page_id_t>(index_id, page_id));
  }

  void *mem = heap->Allocate(sizeof(CatalogMeta));
  auto catalog_meta = new (mem) CatalogMeta(table_meta_pages, index_meta_pages);

  ASSERT(catalog_meta != nullptr, "Invalid Assignment");

  return reinterpret_cast<CatalogMeta *>(mem);
}

uint32_t CatalogMeta::GetSerializedSize() const {
  /* ints: magic_num, table_meta_pages_num, index_meta_pages_num
           table_id, page_id, index_id
  */

  return sizeof(uint32_t) * (3 + 2 * table_meta_pages_.size() + 2 * index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  if (init) {
    // initialize metadata
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    auto cat_meta = buffer_pool_manager->FetchPage(0);
    ASSERT(cat_meta != nullptr, "Invalid Meta Page");
    catalog_meta_->SerializeTo(cat_meta->GetData());
    buffer_pool_manager->FlushPage(0);
    buffer_pool_manager->UnpinPage(0, true);
  } else  // load tableinfo and indexinfo
  {
    auto cat_meta_page = buffer_pool_manager->FetchPage(0);
    ASSERT(cat_meta_page != nullptr, "Invalid Cat Meat");
    catalog_meta_= CatalogMeta::DeserializeFrom(cat_meta_page->GetData(), heap_);
    ASSERT(catalog_meta_ != nullptr, "Invalid Meta Fetch");

    for (auto &it : catalog_meta_->table_meta_pages_) LoadTable(it.first, it.second);

    for (auto &it : catalog_meta_->index_meta_pages_) LoadIndex(it.first, it.second);
  }
}
CatalogManager::~CatalogManager() {
  auto & table_metas = catalog_meta_->table_meta_pages_;
  auto & index_metas = catalog_meta_->index_meta_pages_;
  TableInfo* table_info = nullptr;
  IndexInfo* index_info = nullptr;


  for(auto & idx : index_metas) {
    ASSERT(!buffer_pool_manager_->IsPageFree(idx.second), "Invalid Index Meta");
    index_info = indexes_[idx.first];
    index_info->UpdateRootId();

    auto idx_meta = buffer_pool_manager_->FetchPage(idx.second);
    ASSERT(idx_meta != nullptr, "NULL index meta");
    index_info->meta_data_->SerializeTo(idx_meta->GetData());
    buffer_pool_manager_->FlushPage(idx.second);
    buffer_pool_manager_->UnpinPage(idx.second, false);
  }

  for(auto &tb: table_metas) {
    ASSERT(!buffer_pool_manager_->IsPageFree(tb.second), "Invalid Table Meta");
    table_info = tables_[tb.first];
    table_info->UpdateTableMeta();

    auto tb_meta = buffer_pool_manager_->FetchPage(tb.second);
    ASSERT(tb_meta != nullptr, "NULL table meta");
    buffer_pool_manager_->FlushPage(tb.second);
    buffer_pool_manager_->UnpinPage(tb.second, false);
  }

  auto db_meta = buffer_pool_manager_->FetchPage(0);
  ASSERT(db_meta != nullptr, "Invalid db meta");
  catalog_meta_->SerializeTo(db_meta->GetData());
  buffer_pool_manager_->FlushPage(0);
  buffer_pool_manager_->UnpinPage(0, false);


}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  ASSERT(table_info == nullptr, "Passing UnNull table info");

  if (table_names_.count(table_name)) return DB_TABLE_ALREADY_EXIST;

  auto table_id = next_table_id;
  next_table_id++;

  ASSERT(!tables_.count(table_id), "Duplicate table id");

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, heap_);

  ASSERT(table_meta != nullptr, "Invalid table meta");
  table_info = TableInfo::Create(heap_);
  table_info->Init(table_meta, table_heap);

  page_id_t table_meta_page_id = INVALID_PAGE_ID;
  auto table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  ASSERT(table_meta_page_id != INVALID_PAGE_ID && table_meta_page != nullptr, "invalid meat");

  table_meta->SerializeTo(table_meta_page->GetData());

  catalog_meta_->table_meta_pages_.insert(std::make_pair(table_id, table_meta_page_id));
  tables_.insert(std::make_pair(table_id, table_info));
  table_names_.insert(std::make_pair(table_name, table_id));

  FlushCatalogMetaPage();
  buffer_pool_manager_->FlushPage(table_meta_page_id);
  buffer_pool_manager_->UnpinPage(table_meta_page_id, false);

  std::unordered_map<std::string, std::size_t> column_indexes;
  for(std::size_t i=0; i<table_info->GetSchema()->GetColumnCount(); i++) {
    column_indexes.insert(std::make_pair(table_info->GetSchema()->GetColumns()[i]->GetName(), i));
  }

  std::swap(table_column_indexes[table_info->GetTableName()], column_indexes);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto table_names_it = table_names_.find(table_name);

  if (table_names_it == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  else {
    table_info = tables_.find(table_names_it->second)->second;
    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.size() == 0) {
    return DB_TABLE_NOT_EXIST;
  }

  auto tables_it = tables_.begin();

  for (; tables_it != tables_.end(); ++tables_it) {
    tables.push_back(tables_it->second);
    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // whether the table exists
  if (!table_names_.count(table_name)) return DB_TABLE_NOT_EXIST;

  // whether the index exists in the table
  if (index_names_[table_name].count(index_name)) return DB_INDEX_ALREADY_EXIST;

  // whether all the column names of the index exist

  // get the id of the table
  table_id_t table_id = table_names_[table_name];

  // get information of the table
  TableInfo *table_info = tables_[table_id];

  // get the schema of the table
  Schema *schema = table_info->GetSchema();

  // search for index keys in the schema
  auto column_ = schema->GetColumns();

  for (uint32_t i = 0; i < index_keys.size(); ++i) {
    bool find = false;
    string Index_name = index_keys[i];
    for (uint32_t j = 0; j < column_.size(); ++j) {
      if (column_[j]->GetName() == Index_name) {
        find = true;
        break;
      }
    }
    if (find == false)  // one of index keys not found
    {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }

  // create index

  // get index id
  index_id_t index_id = next_index_id_;
  ++next_index_id_;

  ASSERT(!indexes_.count(index_id), "Duplicate Index");

  // add it to the index names map
  //  tables_indexes_it->second[index_name] = index_id;
  index_names_[table_name].insert(std::make_pair(index_name, index_id));

  // get key map
  std::vector<uint32_t> key_map;

  for (uint32_t i = 0; i < index_keys.size(); ++i) {
    string key_name = index_keys[i];
    for (uint32_t j = 0; j < column_.size(); ++j) {
      if (column_[j]->GetName() == key_name) {
        key_map.push_back(j);
      }
    }
  }

  // get index metadata
  IndexMetadata *meta_data = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap_);

  // get table information
  table_info = tables_.find(table_id)->second;

  // create index information
  index_info = IndexInfo::Create(heap_);
  index_info->Init(meta_data, table_info, buffer_pool_manager_);

  // add it to the index map
  indexes_[index_id] = index_info;

  page_id_t index_meta_page_id = INVALID_PAGE_ID;
  auto index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  ASSERT(index_meta_page_id != INVALID_PAGE_ID && index_meta_page != nullptr, "Invalid Index Meta");

  meta_data->SerializeTo(index_meta_page->GetData());
  catalog_meta_->index_meta_pages_.insert(std::make_pair(index_id, index_meta_page_id));
  indexes_.insert(std::make_pair(index_id, index_info));
  index_names_[table_name].insert(std::make_pair(index_name, index_id));

  FlushCatalogMetaPage();
  buffer_pool_manager_->FlushPage(index_meta_page_id);
  buffer_pool_manager_->UnpinPage(index_meta_page_id, false);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // whether the table exists
  auto tables_indexes_it = index_names_.find(table_name);

  if (tables_indexes_it == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  // whether the index exists
  auto index_it = tables_indexes_it->second.find(index_name);
  if (index_it == (tables_indexes_it->second.end())) {
    return DB_INDEX_NOT_FOUND;
  }

  else {
    // get index id
    index_id_t index_id = index_it->second;

    // get index information
    index_info = indexes_.find(index_id)->second;

    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // whether the table exists
  auto tables_indexes_it = index_names_.find(table_name);

  if (tables_indexes_it == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  else {
    auto index_it = tables_indexes_it->second.begin();
    for (; index_it != tables_indexes_it->second.end(); ++index_it) {
      index_id_t index_id = index_it->second;
      indexes.push_back(indexes_.find(index_id)->second);
    }
    return DB_SUCCESS;
  }

  return DB_FAILED;
}

void CatalogManager::RemoveIndexesOnTable(const std::string &table_name) {
  ASSERT(table_names_.count(table_name), "Invalid table");
  if (index_names_.count(table_name) == 0) return;
  auto &all_index = index_names_[table_name];
  // IndexRootsPage *index_root_page = reinterpret_cast<IndexRootsPage
  // *>(buffer_pool_manager_->FetchPage(1)->GetData());

  for (auto it : all_index) {
    auto index_id = it.second;
    ASSERT(indexes_.count(index_id), "Invalid index id");
    IndexInfo *index_info = indexes_[index_id];
    index_info->GetIndex()->Destroy();
    if (index_info->GetRootPageId() != INVALID_PAGE_ID) buffer_pool_manager_->DeletePage(index_info->GetRootPageId());

    indexes_.erase(index_id);
    ASSERT(catalog_meta_->index_meta_pages_.count(index_id), "invalid index id");
    auto index_meta_page = catalog_meta_->index_meta_pages_[index_id];
    buffer_pool_manager_->DeletePage(index_meta_page);
    catalog_meta_->index_meta_pages_.erase(index_id);
  }

  all_index.erase(table_name);
  buffer_pool_manager_->UnpinPage(1, true);
}

dberr_t CatalogManager::DropTable(const string &table_name, bool remove_index) {
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  auto table_id = table_names_[table_name];
  auto table_info = tables_[table_id];
  auto table_meta_page_id = catalog_meta_->table_meta_pages_[table_id];
  table_info->GetTableHeap()->FreeHeap(true);

  if(remove_index)
    RemoveIndexesOnTable(table_name);

  ASSERT(buffer_pool_manager_->IsPageFree(table_meta_page_id) == false, "Missing Meta Page");

  // remove table's meta page
  buffer_pool_manager_->DeletePage(table_meta_page_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  tables_.erase(table_id);
  table_names_.erase(table_name);

  FlushCatalogMetaPage();

  table_column_indexes.erase(table_name);

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name, bool update_meta) {
  if(table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  if(index_names_[table_name].count(index_name) == 0)
    return DB_INDEX_NOT_FOUND;

  auto index_id = index_names_[table_name][index_name];
  auto index_info = indexes_[index_id];
  index_info->GetIndex()->Destroy();
  ASSERT(buffer_pool_manager_->IsPageFree(index_info->GetRootPageId()), "Invalid Deletion");

  auto index_meta_page_id = catalog_meta_->index_meta_pages_[index_id];
  ASSERT(!buffer_pool_manager_->IsPageFree(index_meta_page_id), "Invalid meta");
  buffer_pool_manager_->DeletePage(index_meta_page_id);
  catalog_meta_->index_meta_pages_.erase(index_id);
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

dberr_t CatalogManager::SerializeToCatalogMetaPage() const {
  // Serialize new catalog meta to the CATALOG_META_PAGE

  // get the page to be written
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);

  // Serialize catalog meta
  char *buf = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(buf);

  // unpin the page
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

  return DB_SUCCESS;
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  //  bool b = buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  //  ASSERT(b, "CatalogMetaPage Pinned");
  //
  //  return DB_SUCCESS;
  auto meta_page = buffer_pool_manager_->FetchPage(0);
  char *buffer = meta_page->GetData();
  catalog_meta_->SerializeTo(buffer);
  buffer_pool_manager_->FlushPage(0);
  buffer_pool_manager_->UnpinPage(0, false);

  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // create table_info
  auto *table_info = TableInfo::Create(heap_);
  TableMetadata *table_meta = nullptr;

  auto table_meta_page = buffer_pool_manager_->FetchPage(page_id);
  ASSERT(table_meta_page != nullptr, "Invalid Fetch");
  TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta, heap_);

  ASSERT(table_meta != nullptr && table_meta->GetFirstPageId() > 0, "Invalid TableHeap First Id");
  ASSERT(table_meta->GetTableId() == table_id, "table id unmatched");

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                            log_manager_, lock_manager_, heap_);
  table_info->Init(table_meta, table_heap);
  table_names_.insert(std::make_pair(table_meta->GetTableName(), table_id));
  tables_.insert(std::make_pair(table_id, table_info));

  if (next_table_id < table_id)
    next_table_id = table_id + 1;
  else
    ++next_index_id_;

  std::unordered_map<std::string, std::size_t> column_indexes;
  for(std::size_t i=0; i<table_info->GetSchema()->GetColumnCount(); i++) {
    column_indexes.insert(std::make_pair(table_info->GetSchema()->GetColumns()[i]->GetName(), i));
  }
//  table_column_indexes[table_info->GetTableName()]
  std::swap(table_column_indexes[table_info->GetTableName()], column_indexes);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // create index_info
  auto *index_info = IndexInfo::Create(heap_);
  IndexMetadata *index_meta = nullptr;

  auto index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  ASSERT(index_meta_page != nullptr, "Invalid Index Meta Fetch");
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta, heap_);

  ASSERT(index_meta != nullptr, "Invalid Index Meta Fetch");
  ASSERT(index_meta->GetIndexId() == index_id, "ID Unmatched");

  auto index_table_id = index_meta->GetTableId();
  ASSERT(tables_.count(index_table_id), "index's table is missing");
  auto index_table_info = tables_[index_table_id];
  index_info->Init(index_meta, index_table_info, buffer_pool_manager_);

  const auto &index_name = index_meta->GetIndexName();
  const auto &table_name = tables_[index_table_id]->GetTableName();

  indexes_.insert(std::make_pair(index_id, index_info));
  index_names_[table_name].insert(std::make_pair(index_name, index_id));
  if (next_index_id_ <= index_id)
    next_index_id_ = index_id + 1;
  else
    ++next_index_id_;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (!tables_.count(table_id)) return DB_TABLE_NOT_EXIST;
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

void CatalogManager::FlushTables() {
  for (auto &table : tables_) {
    ASSERT(catalog_meta_->table_meta_pages_.count(table.first), "Invalid Table Meta Page Id");
    table.second->GetTableHeap()->SaveTable();
    table.second->UpdateTableMeta();
    auto meta_page_id = catalog_meta_->table_meta_pages_[table.first];
    table.second->FlushMetaPage(buffer_pool_manager_, meta_page_id);
  }
}

void CatalogManager::FlushIndexes() {
  for (auto &index : indexes_) {
    ASSERT(catalog_meta_->index_meta_pages_.count(index.first), "Invalid Index Meta Page Id");
    index.second->UpdateRootId();
    auto meta_page_id = catalog_meta_->index_meta_pages_[index.first];
    index.second->FlushMetaPage(buffer_pool_manager_, meta_page_id);
  }
}
