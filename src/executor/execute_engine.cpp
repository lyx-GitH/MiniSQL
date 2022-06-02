#include "executor/execute_engine.h"
#include "common/IntervalMerge.h"
#include "common/comparison.h"
#include "common/format_print.h"
#include "glog/logging.h"

static const std::string db_file_posfix{".db"};
static const std::filesystem::path db_root_dir = std::filesystem::current_path() / "database";
PseudoDataBases ExecuteEngine::database_structure;

ExecuteEngine::ExecuteEngine() {
  std::cout << "MiniSQL init ..." << std::endl;
  std::cout << "working dir: " << db_root_dir.string() << std::endl;
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
    ENABLE_ERROR << "database  " << db_name << " not exist" << DISABLED;
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
  vector<vector<string>> grid;

  int i = 0;
  for (auto &db_name : dbs_) {
    vector<string> line;
    line.push_back(to_string(i));
    line.push_back(db_name.first);
    grid.push_back(std::move(line));
    ++i;
  }
  format_print(grid);
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
  std::cout << "database changed: " << current_db_ << std::endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif

  if (current_db_.empty()) {
    ENABLE_ERROR << "Current Database Not Assigned" << DISABLED;
  };

  vector<vector<string>> grid;

  int i = 0;
  for (auto &table : database_structure[current_db_]) {
    vector<string> line;
    line.push_back(to_string(i));
    line.push_back(table.first);
    grid.push_back(std::move(line));
    ++i;
  }

  format_print(grid);

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

  auto cur = ast;
  std::string table_name{cur->child_->val_};
  cur = cur->child_->next_->child_;
  if (database_structure[current_db_].count(table_name)) {
    ENABLE_ERROR << "table " << table_name << " already exists" << DISABLED;
    return DB_TABLE_ALREADY_EXIST;
  }

  if (!parse_column_definitions(table_name, cur)) {
    //    ENABLE_ERROR << "create table failed" << DISABLED;
    return DB_FAILED;
  }

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
  if (target_db->catalog_mgr_->DropTable(std::string{ast->child_->val_}) != DB_SUCCESS) return DB_TABLE_NOT_EXIST;
  database_structure[current_db_].erase(std::string{ast->child_->val_});
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

  vector<vector<string>> grid;
  int i = 0;

  grid.push_back({"No", "Index Name", "Table Name"});
  for (auto &table : database_structure[current_db_]) {
    for (auto &index : table.second) {
      vector<string> line;
      line.push_back(to_string(i));
      line.push_back(index.first);
      line.push_back(table.first);
      grid.push_back(std::move(line));
      i++;
    }
  }

  format_print(grid, true);

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
  if (target_db->catalog_mgr_->GetTable(table_name, target_table) != DB_SUCCESS) return DB_TABLE_NOT_EXIST;

  const auto &column_indexes = dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_name);
  for (auto &col_name : column_names) {
    if (!column_indexes.count(col_name)) {
      ENABLE_ERROR << "column " << col_name << " not exist" << DISABLED;
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    auto col_idx = column_indexes.at(col_name);
    if (!target_table->GetSchema()->GetColumn(col_idx)->IsUnique()) {
      ENABLE_ERROR << "cannot build index on not-unique column "
                   << target_table->GetSchema()->GetColumn(col_idx)->GetName() << DISABLED;
    }
  }

  ASSERT(target_table != nullptr, "Null Table Fetch");
  IndexInfo *target_index = nullptr;
  if (target_db->catalog_mgr_->CreateIndex(table_name, index_name, column_names, context->txn_, target_index) ==
      DB_FAILED)
    return DB_INDEX_ALREADY_EXIST;

  ASSERT(target_index != nullptr, "Null Index Fetch");

  std::unordered_set<std::string> col_set;
  for (auto &col : column_names) col_set.insert(std::move(col));
  database_structure[current_db_][table_name].insert(std::make_pair(index_name, std::move(col_set)));

  IndexInfo *index_info = nullptr;
  target_db->catalog_mgr_->GetIndex(table_name, index_name, index_info);
  ASSERT(index_info != nullptr, "Invalid Fetch");

  std::unordered_set<RowId> ans_set;
  target_table->GetTableHeap()->FetchAllIds(ans_set);

  batch_index_insert(index_info, target_table, ans_set);

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
  if (current_db_.empty()) {
    ENABLE_ERROR << " current database not assigned" << DISABLED;
    return DB_FAILED;
  }
  auto col_node = ast->child_;
  std::string table_name{col_node->next_->val_};
  TableInfo *table_info = nullptr;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    ENABLE_ERROR << " table " << table_name << " not exist" << DISABLED;
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string, std::size_t> table_column_names;
  std::vector<std::string> used_columns;
  int i = 0;
  for (auto &col : table_info->GetSchema()->GetColumns()) {
    table_column_names.insert(std::make_pair(col->GetName(), i));
    ++i;
  }

  if (col_node->type_ != kNodeAllColumns) {
    ASSERT(col_node->type_ == kNodeColumnList, "Wrong node type");
    for (auto node = col_node->child_; node != nullptr; node = node->next_) {
      std::string col_name{node->val_};
      if (!table_column_names.count(col_name)) {
        ENABLE_ERROR << "column " << col_name << " not exist" << DISABLED;
        return DB_COLUMN_NAME_NOT_EXIST;
      }
      used_columns.push_back(std::move(col_name));
    }
  }else {
    for (auto &col : table_info->GetSchema()->GetColumns()) used_columns.push_back(col->GetName());
  }
  std::unordered_set<RowId> ans_set;
  pSyntaxNode condition_node = col_node->next_->next_;
  if (!condition_node) {
    // fetch a;; ids
    table_info->GetTableHeap()->FetchAllIds(ans_set);
  } else {
    if (!parse_condition(condition_node->child_, table_info, ans_set)) return DB_FAILED;
  }

  pretty_print(table_info, used_columns, table_column_names, ans_set);

  return DB_SUCCESS;
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
  if (target_db->catalog_mgr_->GetTable(table_name, tb_info) != DB_SUCCESS) {
    ENABLE_ERROR << "table " << target_db << " not exist" << DISABLED;
    return DB_TABLE_NOT_EXIST;
  }

  ASSERT(tb_info != nullptr, "Null Table");

  // auto &table_columns = tb_info->GetSchema()->GetColumns();
  //  std::unordered_map<std::string, std::size_t> column_index;
  auto &column_index = dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_name);

  std::vector<Field> data_tuple{};
  make_db_tuple(cur, *tb_info->GetSchema(), column_index, data_tuple);
  if (data_tuple.empty()) {
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

  tb_info->UpdateTableMeta();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  if (current_db_.empty()) {
    ENABLE_ERROR << " current database not assigned" << DISABLED;
    return DB_FAILED;
  }
  std::string table_name{ast->child_->val_};
  if (!database_structure[current_db_].count(table_name)) {
    ENABLE_ERROR << "table " << table_name << " not exist" << DISABLED;
    return DB_TABLE_NOT_EXIST;
  }
  ast = ast->child_->next_;
  TableInfo *table_info = nullptr;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info);
  ASSERT(table_info != nullptr, "Invalid Fetch");

  if (!ast)  // delete every thing
  {
    // destroy the table heap
    IndexInfo *index_info;
    for (auto &idx : database_structure[current_db_][table_name]) {
      dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, idx.first, index_info);
      index_info->GetIndex()->Destroy();
    }
    table_info->GetTableHeap()->FreeHeap();

  } else {
    auto &column_index = dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_name);
    ASSERT(!column_index.empty(), "invalid");

    std::unordered_set<RowId> toRemove;
    if (!parse_condition(ast->child_, table_info, toRemove)) return DB_FAILED;
    auto table_heap = table_info->GetTableHeap();

    for (auto &rid : toRemove) {
      Row data(rid);
      if (table_heap->GetTuple(&data, nullptr) == false) ASSERT(false, "error when parsing conditions");
      update_index(table_name, data.GetRowId(), data.GetFields(), column_index, false);
      table_heap->ApplyDelete(rid, nullptr);
    }
    std::cout << toRemove.size() << " rows effected" << std::endl;
  }

  table_info->UpdateTableMeta();

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if (current_db_.empty()) {
    ENABLE_ERROR << "current database is empty" << DISABLED;
    return DB_FAILED;
  }

  ast = ast->child_;
  std::string table_name{ast->val_};
  TableInfo *table_info;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_FAILED) {
    ENABLE_ERROR << "table " << table_name << " not exists" << DISABLED;
    return DB_TABLE_NOT_EXIST;
  }

  std::unordered_map<std::string, std::size_t> column_index =
      dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_name);
  std::map<std::string, Field> updated;

  ASSERT(column_index.empty() == false, "invalid");

  auto update_node = ast->next_->child_;

  // fetch updated values
  ASSERT(update_node->type_ == kNodeUpdateValue, "Wrong Type");
  while (update_node) {
    auto f = get_field(update_node->child_, table_info);
    std::string col_name{update_node->child_->val_};
    if (f.GetTypeId() == kTypeInvalid) {
      ENABLE_ERROR << "column " << col_name << "not exist";
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    updated.insert(std::make_pair(std::move(col_name), f));
    update_node = update_node->next_;
  }

  auto cond_node = ast->next_->next_;
  std::unordered_set<RowId> ans_set;
  if (!cond_node) {
    table_info->GetTableHeap()->FetchAllIds(ans_set);
  } else {
    if (!parse_condition(cond_node->child_, table_info, ans_set)) return DB_COLUMN_NAME_NOT_EXIST;
  }
  // now we have all the effected rows.
  do_update(table_info, updated, ans_set, column_index);
  table_info->UpdateTableMeta();
  std::cout << ans_set.size() << " rows effected" << std::endl;
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

void ExecuteEngine::make_db_tuple(pSyntaxNode head, const Schema &schema,
                                  std::unordered_map<std::string, std::size_t> &column_index, std::vector<Field> &tup) {
  auto &table_columns = schema.GetColumns();
  auto cur = head;
  std::size_t i = 0;
  //  std::vector<Field> tup;

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
        if (table_columns[i]->GetType() != kTypeFloat && table_columns[i]->GetType() != kTypeInt) goto ERROR;
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
    // column_index.insert(std::make_pair(table_columns[i]->GetName(), i));
  }
  return;

ERROR:
  tup.clear();
}

bool ExecuteEngine::check_index_constrains(const std::string &table_name, const std::vector<Field> &data_tuple,
                                           std::unordered_map<std::string, std::size_t> &column_index) {
  auto &indexes = database_structure[current_db_][table_name];
  auto db_engine = dbs_[current_db_];
  std::vector<Field> key_fields;
  std::vector<RowId> results;
  IndexInfo *index = nullptr;
  for (auto it = indexes.begin(); it != indexes.end(); ++it) {
    if (db_engine->catalog_mgr_->GetIndex(table_name, it->first, index) == DB_SUCCESS) {
      ASSERT(index != nullptr, "Invalid Fetch");
      key_fields.clear();
      for (auto &col_name : index->GetIndexKeySchema()->GetColumns()) {
        std::string name{col_name->GetName()};
        key_fields.push_back(data_tuple[column_index[name]]);
      }
      if (index->GetIndex()->ScanKey(Row(key_fields), results, nullptr) == DB_SUCCESS) return false;
    }
  }

  return true;
}

void ExecuteEngine::update_index(const string &table_name, const RowId &rid, const vector<Field> &data_tuple,
                                 unordered_map<std::string, std::size_t> &column_index, bool insert) {
  auto &indexes = database_structure[current_db_][table_name];
  auto db_engine = dbs_[current_db_];
  std::vector<Field> key_fields;
  std::vector<RowId> results;
  IndexInfo *index = nullptr;
  for (auto it = indexes.begin(); it != indexes.end(); ++it) {
    if (db_engine->catalog_mgr_->GetIndex(table_name, it->first, index) == DB_SUCCESS) {
      ASSERT(index != nullptr, "Invalid Fetch");
      key_fields.clear();
      for (auto &col_name : index->GetIndexKeySchema()->GetColumns()) {
        std::string name{col_name->GetName()};
        key_fields.push_back(data_tuple[column_index[name]]);
      }
      if (insert)
        index->GetIndex()->InsertEntry(Row(key_fields), rid, nullptr);
      else
        index->GetIndex()->RemoveEntry(Row(key_fields), rid, nullptr);
    }
  }
}

void ExecuteEngine::update_index(const string &table_name, const RowId &rid, const vector<Field *> &data_tuple,
                                 unordered_map<std::string, std::size_t> &column_index, bool insert) {
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
        key_fields.push_back(*data_tuple[column_index[name]]);
      }
      if (insert)
        index->GetIndex()->InsertEntry(Row(key_fields), rid, nullptr);
      else
        index->GetIndex()->RemoveEntry(Row(key_fields), rid, nullptr);
    }
  }
}

void ExecuteEngine::batch_index_insert(IndexInfo *index_info, TableInfo *table_info,
                                       std::unordered_set<RowId> &ans_set) {
  const auto &column_index = dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_info->GetTableName());
  std::vector<Field> key_fields;
  for (auto rid : ans_set) {
    key_fields.clear();
    Row row(rid);
    table_info->GetTableHeap()->GetTuple(&row, nullptr);

    for (auto &col_name : index_info->GetIndexKeySchema()->GetColumns()) {
      std::string name{col_name->GetName()};
      ASSERT(column_index.count(name), "invalid cilumn name");
      key_fields.push_back(*row.GetFields()[column_index.at(name)]);
    }
    index_info->GetIndex()->InsertEntry(Row(key_fields), rid, nullptr);
  }
}

bool ExecuteEngine::parse_column_definitions(const string &table_name, pSyntaxNode head) {
  if (!head) return false;

  TableInfo *table_info = nullptr;
  IndexInfo *index_info = nullptr;
  std::vector<Column *> table_defs;
  std::unordered_map<std::string, std::size_t> column_index;

  std::size_t i = 0;

  while (head && head->type_ != kNodeColumnList) {
    bool is_unique = false;
    bool is_nullable = true;
    if (head->val_ && strcmp(head->val_, "unique") == 0)  // this is a unique
      is_unique = true;
    else if (head->val_ && strcmp(head->val_, "not null") == 0)  // not null
      is_nullable = false;
    Column *column = parse_single_column(head->child_, i, is_nullable, is_unique);

    if (!column) {
      for (auto col : table_defs) delete col;
      return false;
    }

    table_defs.push_back(column);
    column_index.insert(std::make_pair(column->GetName(), i));

    head = head->next_;
    i++;
  }

  //  void *buf = mem_heap->Allocate(sizeof(TableSchema));
  auto *tb_schema = new TableSchema(table_defs);

  dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, tb_schema, nullptr, table_info);
  if (!table_info) goto ERROR;
  database_structure[current_db_].insert(std::make_pair(table_name, PseudoIndex()));

  if (head) {
    ASSERT(head->type_ == kNodeColumnList, "Unexpected Syntax Tree Structure");
    head = head->child_;
    std::vector<std::string> pm_keys;
    std::unordered_set<std::string> key_set;

    // generate primary key
    while (head) {
      pm_keys.emplace_back(head->val_);
      key_set.insert(head->val_);
      head = head->next_;
    }
    //    bool unique = false;
    for (auto &key_name : pm_keys) {
      if (column_index.count(key_name) == 0) {
        dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
        goto ERROR;
      }
    }

    dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, "_primary_keys", pm_keys, nullptr, index_info);
    database_structure[current_db_][table_name].insert(std::make_pair("_primary_keys", std::move(key_set)));
  }
  for (auto col : table_defs) {
    if (col->IsUnique()) {
      // create an index for the unique key.
      dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, col->GetName(), {col->GetName()}, nullptr, index_info);
      database_structure[current_db_][table_name][col->GetName()] = {col->GetName()};
    }
  }
  table_info->UpdateTableMeta();
  return true;

ERROR:
  for (auto col : table_defs) delete col;
  return false;
}

// parse a single column
Column *ExecuteEngine::parse_single_column(pSyntaxNode ast, const int column_position, bool is_nullable,
                                           bool is_unique) {
  if (!ast) return nullptr;
  auto column_name = std::string{ast->val_};
  ast = ast->next_;
  auto type_name = std::string{ast->val_};
  ast = ast->child_;

  if (type_name == "float") return new Column(column_name, TypeId::kTypeFloat, column_position, is_nullable, is_unique);

  if (type_name == "int") return new Column(column_name, TypeId::kTypeInt, column_position, is_nullable, is_unique);

  if (type_name == "char" && ast != nullptr) {
    auto num_in_str = std::string{ast->val_};
    for (auto &c : num_in_str)
      if (!isdigit(c)) return nullptr;
    int len = atoi(num_in_str.c_str());
    // string is too long
    if (len <= 0 || len >= 0xFF) return nullptr;
    return new Column(column_name, TypeId::kTypeChar, len, column_position, is_nullable, is_unique);
  }

  return nullptr;
}
bool ExecuteEngine::parse_condition(pSyntaxNode ast, const TableInfo *table_info, std::unordered_set<RowId> &ans_set) {
  if (ast->type_ == kNodeCompareOperator) {
    return parse_compare(ast, table_info, ans_set);
  } else if (ast->type_ == kNodeConnector) {
    std::unordered_set<RowId> other;
    bool b1 = parse_condition(ast->child_, table_info, ans_set);
    bool b2 = parse_condition(ast->child_->next_, table_info, other);
    if (b1 == false || b2 == false) return false;
    if (strcmp(ast->val_, "and") == 0)
      set_and(ans_set, other);
    else if (strcmp(ast->val_, "or") == 0)
      set_or(ans_set, other);
    return true;

  } else
    goto ERROR;
ERROR:
  ASSERT(false, "Unexpected Syntax Tree Structure");
}

bool ExecuteEngine::parse_compare(pSyntaxNode ast, const TableInfo *table_info, std::unordered_set<RowId> &ans_set) {
  ASSERT(ast->type_ == kNodeCompareOperator, "Wrong Type");
  std::string compare_token{ast->val_};
  std::string key_column_name{ast->child_->val_};

  ASSERT(comparisons.count(compare_token) != 0, "Invalid compare token");
  // std::string compare_key_str{ast->child_->next_->val_};
  uint32_t key_index;
  if (table_info->GetSchema()->GetColumnIndex(key_column_name, key_index) != DB_SUCCESS) {
    ENABLE_ERROR << "column " << key_column_name << "not exist" << DISABLED;
    return false;
  }

  IndexInfo *index_info = find_index(table_info, key_column_name);
  Field key_field = get_field(ast->child_, table_info);

  if (key_field.GetTypeId() == kTypeInvalid) return false;

  if (index_info && compare_token == "=")  // has a index '='    ueey, just scan key.
  {
    std::vector<Field> f;
    f.push_back(key_field);
    index_info->GetIndex()->ScanKey(Row(f), ans_set);
  } else if (!index_info || idx_comps.count(compare_token) == 0)  // no index, or the token cannot be proccssed by index
  {
    table_info->GetTableHeap()->FetchId(ans_set, key_index, table_info->GetSchema(), key_field,
                                        comparisons.find(compare_token)->second);
  } else  // token can be done
  {
    std::vector<Field> f;
    f.push_back(key_field);
    auto cmp_args = idx_comps.find(compare_token)->second;
    index_info->GetIndex()->RangeScanKey(Row(f), ans_set, cmp_args.left, cmp_args.key_included);
  }

  return true;
}

Field ExecuteEngine::get_field(pSyntaxNode ast, const TableInfo *table_info) {
  pSyntaxNode val_node = ast->next_;
  std::string column_name{ast->val_};
  // std::string field_value{val_node->val_};

  //  uint32_t column_index;
  //  if (table_info->GetSchema()->GetColumnIndex(column_name, column_index) == DB_FAILED) return Field(kTypeInvalid);
  const std::string &table_name = table_info->GetTableName();
  if (!dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_name).count(column_name))
    return Field(kTypeInvalid);
  uint32_t column_index = dbs_[current_db_]->catalog_mgr_->GetTableColumnIndexes(table_name).at(column_name);

  const Column *column = table_info->GetSchema()->GetColumn(column_index);
  if (val_node->type_ == kNodeNull && column->IsNullable())
    return Field(column->GetType());
  else if (val_node->type_ == kNodeString && column->GetType() == kTypeChar)
    return Field(kTypeChar, val_node->val_, strlen(val_node->val_), true);
  else if (val_node->type_ == kNodeNumber && column->GetType() == kTypeFloat)
    return Field(kTypeFloat, (float)(atof(val_node->val_)));
  else if (val_node->type_ == kNodeNumber && column->GetType() == kTypeInt)
    return Field(kTypeInt, atoi(val_node->val_));
  else
    return Field(kTypeInvalid);
}

IndexInfo *ExecuteEngine::find_index(const TableInfo *table_info, const std::string &column_name) {
  IndexInfo *index_info = nullptr;
  // scan the index that only contains *that* column;
  for (auto &it : database_structure[current_db_][table_info->GetTableName()]) {
    if (it.second.size() == 1 && it.second.count(column_name) != 0) {
      auto res = dbs_[current_db_]->catalog_mgr_->GetIndex(table_info->GetTableName(), it.first, index_info);
      ASSERT(res != DB_FAILED, "Invalid index fetch");
      break;
    }
  }
  return index_info;
}

void ExecuteEngine::pretty_print(TableInfo *table_info, std::vector<std::string> &used_columns,
                                 std::unordered_map<std::string, std::size_t> &column_index,
                                 std::unordered_set<RowId> &ans_set) {
  std::vector<std::vector<std::string>> grid;
  grid.push_back(used_columns);
  if (ans_set.empty()) return;
  for (auto &rid : ans_set) {
    vector<string> r;
    Row row(rid);
    table_info->GetTableHeap()->GetTuple(&row, nullptr);
    for (uint32_t i = 0; i < used_columns.size(); i++) {
      auto col_idx = column_index[used_columns[i]];
      r.push_back(row.GetField(col_idx)->toString());
    }
    grid.push_back(std::move(r));
  }

  format_print(grid, true);
}
void ExecuteEngine::do_update(const TableInfo *table_info, map<string, Field> new_values,
                              unordered_set<RowId> effected_rows, unordered_map<string, size_t> column_index) {
  auto table_heap = table_info->GetTableHeap();
  for (auto &rid : effected_rows) {
    Row cur_row(rid);
    auto res = table_heap->GetTuple(&cur_row, nullptr);
    ASSERT(res, "Invalid Tuple Fetch");
    update_index(table_info->GetTableName(), cur_row.GetRowId(), cur_row.GetFields(), column_index, false);
    auto &old_values = cur_row.GetFields();

    for (auto &it : new_values) {
      if (new_values.count(it.first)) {
        auto col_index = column_index[it.first];
        old_values[col_index]->DeepCopy(it.second);
      }
    }
    if (!table_heap->UpdateTuple(cur_row, cur_row.GetRowId(), nullptr)) {
      table_heap->ApplyDelete(cur_row.GetRowId(), nullptr);
      table_heap->InsertTuple(cur_row, nullptr);
    }

    update_index(table_info->GetTableName(), cur_row.GetRowId(), cur_row.GetFields(), column_index, true);
  }
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
