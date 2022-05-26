#include "executor/execute_engine.h"
#include "glog/logging.h"

static const std::string db_file_posfix{".db"};
static const std::filesystem::path db_root_dir = std::filesystem::current_path() / "database";
PseudoDataBases ExecuteEngine::database_structure;

ExecuteEngine::ExecuteEngine() {
  if (!filesystem::exists(db_root_dir)) filesystem::create_directories(db_root_dir);
  std::filesystem::directory_iterator db_files(db_root_dir);
  // list all files
  for (auto &file : db_files) {
    std::string file_name = file.path().filename().string();
    if (file_name.find(db_file_posfix) != std::string::npos)  // this is a db_file
    {
      auto database = new DBStorageEngine(file.path(), false);
      dbs_.insert(std::make_pair(file_name, database));
      generate_db_struct(file_name, database);
      std::cout << "database found: " << file_name << std::endl;
      std::cout << file.path()<<std::endl;
    }
  }
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif

  ASSERT(ast->child_ != nullptr, "Unexpected Tree Structure");
  std::string db_name{ast->child_->val_};
  db_name.append(db_file_posfix);
  if (dbs_.count(db_name) != 0) {
    ENABLE_ERROR << "database " << db_name << " already exists" << DISABLED;
    return DB_FAILED;
  }

  auto database = new DBStorageEngine(db_root_dir / db_name);
  dbs_.insert(std::make_pair(db_name, database));
  generate_db_struct(db_name, database);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  ASSERT(ast->child_ != nullptr, "Unexpected Tree Structure");
  std::string db_name{ast->child_->val_};
  db_name.append(db_file_posfix);

  if (dbs_.count(db_name) == 0) {
    ENABLE_ERROR << "No Such Database: " << db_name << DISABLED;
    return DB_FAILED;
  }

  delete dbs_[db_name];
  dbs_.erase(db_name);
  database_structure.erase(db_name);
  if (current_db_ == db_name) current_db_.clear();
  if (std::filesystem::exists(db_root_dir / db_name)) {
    std::filesystem::remove(db_root_dir / db_name);
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif

  int i = 0;
  for (auto &db_name : dbs_) {
    COUT_ALIGN(10) << '|' << i << '|' << db_name.first << '|' << std::endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif

  std::string db_name{ast->child_->val_};
  db_name.append(db_file_posfix);
  if (dbs_.count(db_name) == 0) {
    ENABLE_ERROR << "No Such Database: " << db_name << DISABLED;
    return DB_FAILED;
  }
  std::swap(current_db_, db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif

  if (current_db_.empty()) {
    ENABLE_ERROR << "Current Database Not Assigned" << DISABLED;
  };

  //  auto current_engine = dbs_.find(current_db_)->second;
  //  vector<TableInfo *> all_tables = {};
  //  auto fetch_success = current_engine->catalog_mgr_->GetTables(all_tables);
  //  if (fetch_success == DB_FAILED) return DB_TABLE_NOT_EXIST;
  //  int i = 0;
  //  for (auto table : all_tables) {
  //    COUT_ALIGN(10) << '|' << i << '|' << table->GetTableName() << std::endl;
  //    i++;
  //    delete table;
  //  }
  int i = 0;
  for (auto &table : database_structure[current_db_]) {
    COUT_ALIGN(10) << '|' << i << '|' << table.first << std::endl;
    ++i;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) return DB_FAILED;
  ASSERT(ast->child_ != nullptr, "Unexpected Tree Structure");

  if (current_db_.empty()) {
    return DB_FAILED;
  }

  //  auto db_mng = dbs_[current_db_];
  //  auto table_def_node = ast->child_;
  //  std::string table_name{table_def_node->val_};

  //  Schema* schema = new Schema();

  // if (table_name.empty()) return DB_FAILED;

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    ENABLE_ERROR << " current database is empty" << DISABLED;
    return DB_FAILED;
  }
  auto target_db = dbs_.find(current_db_)->second;
  if (target_db->catalog_mgr_->DropTable(std::string{ast->val_}) == DB_FAILED) return DB_TABLE_NOT_EXIST;
  database_structure[current_db_].erase(std::string{ast->val_});
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (ast == nullptr) return DB_FAILED;
  if (current_db_.empty()) {
    ENABLE_ERROR << "Current Database Not Assigned" << DISABLED;
    return DB_FAILED;
  }

  int i = 0;
  for (auto &table : database_structure[current_db_]) {
    COUT_ALIGN(10) << '|' << "No." << '|' << "name" << '|' << "table" << std::endl;
    for (auto &index : table.second)
      COUT_ALIGN(10) << '|' << (i++) << '|' << index.first << '|' << table.first << '|' << std::endl;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (!ast) return DB_FAILED;
  if (current_db_.empty()) {
    ENABLE_ERROR << "Current Database Not Assigned" << DISABLED;
    return DB_FAILED;
  }

  auto target_db = dbs_[current_db_];
  auto cur = ast->child_;
  std::string index_name{cur->val_};
  cur = cur->next_;
  std::string table_name{cur->val_};
  cur = cur->next_;
  ASSERT(cur && cur->type_ == kNodeColumnList, "Unexpected Index behaviour");

  if (database_structure[current_db_][table_name].count(index_name) != 0) {
    ENABLE_ERROR << "index " << index_name << " already exists in table " << table_name << DISABLED;
    return DB_INDEX_ALREADY_EXIST;
  }

  std::vector<std::string> column_names;
  cur = cur->child_;
  while (cur) {
    column_names.emplace_back(cur->val_);
    cur = cur->next_;
  }

  ASSERT(column_names.empty() == false, "No Columns Got");

  TableInfo *target_table = nullptr;
  if (target_db->catalog_mgr_->GetTable(table_name, target_table) == DB_FAILED) return DB_TABLE_NOT_EXIST;

  ASSERT(target_table != nullptr, "Null Table Fetch");
  IndexInfo *target_index = nullptr;
  if (target_db->catalog_mgr_->CreateIndex(table_name, index_name, column_names, context->txn_, target_index) ==
      DB_FAILED)
    return DB_INDEX_ALREADY_EXIST;

  ASSERT(target_index != nullptr, "Null Index Fetch");

  std::unordered_set<std::string> col_set;
  for (auto &col : column_names) col_set.insert(std::move(col));
  database_structure[current_db_][table_name].insert(std::make_pair(std::move(index_name), std::move(col_set)));

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (!ast) return DB_FAILED;
  if (current_db_.empty()) {
    ENABLE_ERROR << "Current Database Not Assigned" << DISABLED;
    return DB_FAILED;
  }

  auto target_db = dbs_[current_db_];
  std::string index_name(ast->child_->val_);
  std::string table_name{};

  ASSERT(!index_name.empty(), "Invalid index name");

  auto &tables = database_structure[current_db_];
  for (auto &table : tables) {
    if (table.second.count(index_name)) {
      auto res = target_db->catalog_mgr_->DropIndex(table.first, index_name);
      if (res != DB_FAILED) table.second.erase(index_name);
      return res;
    }
  }
  ENABLE_ERROR << "index " << index_name << " not found" << DISABLED;
  return DB_INDEX_NOT_FOUND;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif

  if (!ast) return DB_FAILED;
  if (current_db_.empty()) {
    ENABLE_ERROR << "Current Database Not Assigned" << DISABLED;
    return DB_FAILED;
  }
  auto cur = ast->child_;
  auto target_db = dbs_[current_db_];
  std::string table_name(cur->val_);
  cur = cur->next_->child_;

  TableInfo *tb_info = nullptr;
  if (target_db->catalog_mgr_->GetTable(table_name, tb_info) == DB_FAILED) {
    ENABLE_ERROR << "table " << target_db << " not exist" << DISABLED;
    return DB_TABLE_NOT_EXIST;
  }

  ASSERT(tb_info != nullptr, "Null Table");

  //auto &table_columns = tb_info->GetSchema()->GetColumns();
  std::unordered_map<std::string, std::size_t> column_index;

  std::vector<Field> data_tuple = make_db_tuple(cur, *tb_info->GetSchema(), column_index);
  if (data_tuple.empty()) {
    delete tb_info;
    ENABLE_ERROR << "insertion failed (data types unmatched)" << DISABLED;
    return DB_FAILED;
  }

  if (!check_index_constrains(table_name, data_tuple, column_index)) {
    ENABLE_ERROR << "insertion failed (unique key constraints violated)" << DISABLED;
    return DB_FAILED;
  }
  Row data_row(data_tuple);
  if (!tb_info->GetTableHeap()->InsertTuple(data_row, nullptr)) {
    ENABLE_ERROR << "insertion failed (entry too large)" << DISABLED;
    return DB_FAILED;
  }

  update_index(table_name, data_row.GetRowId(), data_tuple, column_index);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}

bool ExecuteEngine::generate_db_struct(const string &db_name, const DBStorageEngine *db) {
  if (!db) return false;
  std::vector<TableInfo *> all_tables;
  std::vector<IndexInfo *> all_indexes;
  // TODO: fix here when catalog_mng is ready.
  db->catalog_mgr_->GetTables(all_tables);
  PseudoTables p_tables;
  for (auto table : all_tables) {
    all_indexes.clear();
    db->catalog_mgr_->GetTableIndexes(table->GetTableName(), all_indexes);
    PseudoIndex p_index;
    for (auto index : all_indexes) {
      std::unordered_set<std::string> col_names;
      for (auto col : index->GetIndexKeySchema()->GetColumns()) col_names.insert(col->GetName());
      p_index.insert(std::make_pair(index->GetIndexName(), std::move(col_names)));
    }
    p_tables.insert(std::make_pair(table->GetTableName(), std::move(p_index)));
  }
  ExecuteEngine::database_structure.insert(std::make_pair(db_name, std::move(p_tables)));

  return true;
}

std::vector<Field> &&ExecuteEngine::make_db_tuple(pSyntaxNode head, const Schema &schema,
                                                  std::unordered_map<std::string, std::size_t> &column_index) {
  auto &table_columns = schema.GetColumns();
  auto cur = head;
  std::size_t i = 0;
  std::vector<Field> tup;

  for (; i < table_columns.size() && cur != nullptr; ++i, cur = cur->next_) {
    switch (cur->type_) {
      case (kNodeString): {
        if (table_columns[i]->GetType() != TypeId::kTypeChar || strlen(cur->val_) > table_columns[i]->GetLength())
          goto ERROR;
        tup.emplace_back(TypeId::kTypeChar, cur->val_, strlen(cur->val_), true);
        break;
      };
      case (kNodeNull): {
        if (table_columns[i]->IsNullable() == false) goto ERROR;
        tup.emplace_back(table_columns[i]->GetType());
        break;
      };

      case (kNodeNumber): {
        if (table_columns[i]->GetType() != kTypeFloat || table_columns[i]->GetType() != kTypeInt) goto ERROR;
        if (table_columns[i]->GetType() == kTypeFloat) {
          tup.emplace_back(TypeId::kTypeFloat, (float)atof(cur->val_));
        } else if (table_columns[i]->GetType() == kTypeInt) {
          tup.emplace_back(TypeId::kTypeInt, atoi(cur->val_));
        }
        break;
      }
      default:
        goto ERROR;
    }
    column_index.insert(std::make_pair(table_columns[i]->GetName(), i));
  }
  return std::move(tup);

ERROR:
  return std::forward<std::vector<Field>>({});
}

bool ExecuteEngine::check_index_constrains(const std::string &table_name, const std::vector<Field> &data_tuple,
                                           std::unordered_map<std::string, std::size_t> &column_index) {
  auto &indexes = database_structure[current_db_][table_name];
  auto db_engine = dbs_[current_db_];
  std::vector<Field> key_fields;
  std::vector<RowId> results;
  IndexInfo *index = nullptr;
  for (auto it = indexes.begin(); it != indexes.end(); ++it) {
    if (db_engine->catalog_mgr_->GetIndex(table_name, it->first, index) != DB_FAILED) {
      ASSERT(index != nullptr, "Invalid Fetch");
      key_fields.clear();
      for (auto &col_name : index->GetIndexKeySchema()->GetColumns()) {
        std::string name{col_name->GetName()};
        key_fields.push_back(data_tuple[column_index[name]]);
      }
      if (index->GetIndex()->ScanKey(Row(key_fields), results, nullptr) == DB_SUCCESS) {
        delete index;
        return false;
      }
      delete index;
    }
  }

  return true;
}
void ExecuteEngine::update_index(const string &table_name, const RowId &rid, const vector<Field> &data_tuple,
                                 unordered_map<std::string, std::size_t> &column_index) {
  auto &indexes = database_structure[current_db_][table_name];
  auto db_engine = dbs_[current_db_];
  std::vector<Field> key_fields;
  std::vector<RowId> results;
  IndexInfo *index = nullptr;
  for (auto it = indexes.begin(); it != indexes.end(); ++it) {
    if (db_engine->catalog_mgr_->GetIndex(table_name, it->first, index) != DB_FAILED) {
      ASSERT(index != nullptr, "Invalid Fetch");
      key_fields.clear();
      for (auto &col_name : index->GetIndexKeySchema()->GetColumns()) {
        std::string name{col_name->GetName()};
        key_fields.push_back(data_tuple[column_index[name]]);
      }

      index->GetIndex()->InsertEntry(Row(key_fields), rid, nullptr);
      delete index;
    }
  }
}
