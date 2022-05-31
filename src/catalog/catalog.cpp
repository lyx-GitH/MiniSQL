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
  uint32_t magic_number, table_meta_pages_num, index_meta_pages_num;
  uint32_t table_id, page_id, index_id;
  ser_cnt++;
  ser_cnt--;

  std::map<table_id_t, page_id_t> table_meta_pages;
  std::map<index_id_t, page_id_t> index_meta_pages;

  // read and check magic number
  magic_number = MACH_READ_INT32(buf);
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

  return reinterpret_cast<CatalogMeta*>(mem);
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
  if (init == 1) {
    // initialize metadata
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    auto meta = buffer_pool_manager->FetchPage(0);
    catalog_meta_->SerializeTo(meta->GetData());
    buffer_pool_manager->UnpinPage(0, true);
    buffer_pool_manager->FlushPage(0);
  }

  else  // load tableinfo and indexinfo
  {
    // Deserialize catalog meta
    Page* meta_page = buffer_pool_manager->FetchPage(0);
    char *buf = meta_page->GetData();
    CatalogMeta *Catalog_meta_ = CatalogMeta::DeserializeFrom(buf, heap_);
    ASSERT(Catalog_meta_ != nullptr, "invalid meta");
    catalog_meta_ = Catalog_meta_;
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);

    // load tableinfo first
    auto table_meta_it = Catalog_meta_->table_meta_pages_.begin();
    for (; table_meta_it != Catalog_meta_->table_meta_pages_.end(); ++table_meta_it) {
      LoadTable(table_meta_it->first, table_meta_it->second);
    }

    // load indexinfo
    auto index_info_it = Catalog_meta_->index_meta_pages_.begin();
    for (; index_info_it != Catalog_meta_->index_meta_pages_.end(); ++index_info_it) {
      LoadIndex(index_info_it->first, index_info_it->second);
    }
  }
}

CatalogManager::~CatalogManager() {
//  ASSERT(catalog_meta_ != nullptr, "Null Meta");
//  FlushCatalogMetaPage();

//  delete heap_;
    delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // whether the table exists
  auto exists = table_names_.find(table_name);

  if (exists != table_names_.end())  // already exists
  {
    return DB_TABLE_ALREADY_EXIST;
  }

  else  // not exist
  {
    // get table id and add to table names map
    table_id_t table_id = next_table_id_.fetch_add(1);
    table_names_[table_name] = table_id;

    // create new table heap
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);

    // create table metadata
    auto tmd = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, heap_);

    // get a new page to store table meta
    page_id_t pid;
    Page *table_meta_page = buffer_pool_manager_->NewPage(pid);

    // add to table meta pages map -- update catalog metadata
    catalog_meta_->table_meta_pages_[table_id] = pid;

    // write table metadata to the page
    char *buf = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));

    tmd->SerializeTo(buf);

    /*char buf_[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; ++i)
    {
      buf_[i] = buf[i];
    }*/

    table_meta_page->SetData(buf);
    buffer_pool_manager_->UnpinPage(pid, false);

    // create and initialize table info
    table_info = TableInfo::Create(heap_);
    table_info->Init(tmd, table_heap);

    // add to table-index map
    std::unordered_map<std::string, index_id_t> indexes_names;
    index_names_[table_name] = indexes_names;

    tables_[table_names_[table_name]] = table_info;

    // add to tables map
    tables_[table_id] = table_info;

    // update CatalogMetaPage
    SerializeToCatalogMetaPage();

    return DB_SUCCESS;
  }

  return DB_FAILED;
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
  auto tables_indexes_it = index_names_.find(table_name);

  if (tables_indexes_it == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  // whether the index exists in the table
  auto index_it = tables_indexes_it->second.find(index_name);
  if (index_it != (tables_indexes_it->second.end())) {
    return DB_INDEX_ALREADY_EXIST;
  }

  // whether all the column names of the index exist

  // get the id of the table
  table_id_t table_id = table_names_.find(table_name)->second;

  // get information of the table
  TableInfo *table_info = tables_.find(table_id)->second;

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
  index_id_t index_id = next_index_id_.fetch_add(1);

  // add it to the index names map
  tables_indexes_it->second[index_name] = index_id;

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

  // get a new page to store index meta
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);

  // add to index meta pages map -- update catalog metadata
  catalog_meta_->index_meta_pages_[index_id] = page_id;

  // write table metadata to the page
  char *buf = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
  meta_data->SerializeTo(buf);

  /*
  char buf_[PAGE_SIZE];
  for (int i = 0; i < PAGE_SIZE; ++i)
  {
    buf_[i] = buf[i];
  }*/

  page->SetData(buf);
  buffer_pool_manager_->UnpinPage(page_id, false);

  // update CatalogMetaPage
  SerializeToCatalogMetaPage();

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

dberr_t CatalogManager::DropTable(const string &table_name) {
  // check whether table exists
  auto table_it = table_names_.find(table_name);
  if (table_it == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  else  // drop table
  {
    table_id_t table_id = table_it->second;

    // delete in the table names map
    table_names_.erase(table_name);

    // delete in the table map
    tables_.erase(table_id);

    // delete in the table indexes map
    index_names_.erase(table_name);

    // delete the page

    // get page id
    page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];

    // delete
    buffer_pool_manager_->DeletePage(page_id);

    // update CatalogMetaPage
    SerializeToCatalogMetaPage();

    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
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

  else  // drop index
  {
    tables_indexes_it->second.erase(index_it);
    indexes_.erase(index_it->second);

    // delete the page

    // get page id
    page_id_t page_id = catalog_meta_->index_meta_pages_[index_it->second];

    // delete
    buffer_pool_manager_->DeletePage(page_id);

    // update CatalogMetaPage
    SerializeToCatalogMetaPage();

    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::SerializeToCatalogMetaPage() const {
  // Serialize new catalog meta to the CATALOG_META_PAGE

  // get the page to be written
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);

  // Serialize catalog meta
  char *buf = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
  catalog_meta_->SerializeTo(buf);

  // write data to the page
  catalog_meta_page->SetData(buf);

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
    char* buffer = meta_page->GetData();
    catalog_meta_->SerializeTo(buffer);
    buffer_pool_manager_->UnpinPage(0, true);
    buffer_pool_manager_->FlushPage(0);

    return DB_SUCCESS;

}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // create table_info
  auto *table_info = TableInfo::Create(heap_);
  TableMetadata *table_meta = nullptr;

  // get data from page
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  TableMetadata::DeserializeFrom(buf, table_meta, table_info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);

  if (table_meta != nullptr) {
    Schema *sch = table_meta->GetSchema();

    // table_info and table_heap are created by table_meta
    auto *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), sch, log_manager_,
                                         lock_manager_, table_info->GetMemHeap());
    table_info->Init(table_meta, table_heap);

    // add to table names map
    table_names_[table_meta->GetTableName()] = table_id;

    // add to table map
    tables_[table_id] = table_info;

    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {

  // create index_info
  auto *index_info = IndexInfo::Create(heap_);
  IndexMetadata *index_meta = nullptr;

  // get data from page
  char *buf = buffer_pool_manager_->FetchPage(page_id)->GetData();
  IndexMetadata::DeserializeFrom(buf, index_meta, index_info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);

  if (index_meta != nullptr) {
    // get iterator of table info
    table_id_t table_id = index_meta->GetTableId();
    auto table_it = tables_.find(table_id);

    // index_info is created by index_meta and table_info
    index_info->Init(index_meta, table_it->second, buffer_pool_manager_);

    // get table name
    string table_name;
    auto table_name_it = table_names_.begin();
    for (; table_name_it != table_names_.end(); table_name_it++) {
      if (table_name_it->second == table_id) {
        table_name = table_name_it->first;
        break;
      }
    }

    // add to index names map
    std::unordered_map<std::string, index_id_t> index_name;
    index_name[index_meta->GetIndexName()] = index_id;
    index_names_[table_name].insert(std::make_pair(index_meta->GetIndexName(), index_id));

    // add to index map
    indexes_[index_id] = index_info;
    std::cout <<"index "<<index_meta->GetIndexName()<<" loaded"<<std::endl;

    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto tables_it = tables_.find(table_id);

  if (tables_it == tables_.end()) {
    return DB_FAILED;
  }

  else {
    table_info = tables_it->second;
    return DB_SUCCESS;
  }

  return DB_FAILED;
}