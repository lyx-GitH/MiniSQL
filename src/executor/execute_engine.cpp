#include "executor/execute_engine.h"
#include "glog/logging.h"

#define COUT_ALIGN(x) std::cout << setiosflags(ios::adjustfield) << setw(x)

ExecuteEngine::ExecuteEngine() {}

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
  if (dbs_.count(db_name) != 0) {
    std::cout << "Error: Database Exists: " << db_name << std::endl;
    return DB_FAILED;
  }

  auto database = new DBStorageEngine(db_name);
  dbs_.insert(std::make_pair(db_name, database));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  ASSERT(ast->child_ != nullptr, "Unexpected Tree Structure");
  std::string db_name{ast->child_->val_};

  if (dbs_.count(db_name) == 0) {
    std::cout << "Error: No Such Database: " << db_name << std::endl;
    return DB_FAILED;
  }

  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (current_db_ == db_name) current_db_.clear();
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

  std::string db_name{ast->val_};
  if (dbs_.count(db_name) == 0) {
    std::cout << "Error: No Such Database: " << db_name << std::endl;
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
    std::cout << "Error: Current Database Not  Assigned" << std::endl;
  };

  auto current_engine = dbs_.find(current_db_)->second;
  vector<TableInfo *> all_tables = {};
  auto fetch_success = current_engine->catalog_mgr_->GetTables(all_tables);
  if (fetch_success == DB_FAILED) return DB_TABLE_NOT_EXIST;
  int i = 0;
  for (auto table : all_tables) {
    COUT_ALIGN(10) << '|' << i << '|' << table->GetTableName() << std::endl;
    i++;
    delete table;
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
  auto target_db = dbs_.find(current_db_)->second;
  if (target_db->catalog_mgr_->DropTable(std::string{ast->val_}) == DB_FAILED) return DB_TABLE_NOT_EXIST;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (!ast) return DB_FAILED;
  if (current_db_.empty()) {
    std::cout << "Error: Current Database Not Assigned" << std::endl;
    return DB_FAILED;
  }

  auto target_db = dbs_[current_db_];
  std::vector<IndexInfo *> all_indexes;
  std::vector<TableInfo *> all_tables;

  if (target_db->catalog_mgr_->GetTables(all_tables) == DB_FAILED) return DB_FAILED;
  int i;
  for (auto table : all_tables) {
    if (target_db->catalog_mgr_->GetTableIndexes(table->GetTableName(), all_indexes) == DB_FAILED) goto ERROR_TRAP;
    i = 0;
    for (auto index : all_indexes) {
      COUT_ALIGN(10) << '|' << i << '|' << index->GetIndexName() << '|' << table->GetTableName() << '|' << std::endl;
      ++i;
      delete index;
    }

    delete table;
  }

  return DB_SUCCESS;

ERROR_TRAP:
  for (auto table : all_tables) delete table;
  for (auto index : all_indexes) delete index;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (!ast) return DB_FAILED;
  if (current_db_.empty()) {
    std::cout << "Error: Current Database Not Assigned" << std::endl;
    return DB_FAILED;
  }

  auto target_db = dbs_[current_db_];
  auto cur = ast->child_;
  std::string index_name{cur->val_};
  cur = cur->next_;
  std::string table_name{cur->val_};
  cur = cur->next_;
  ASSERT(cur && cur->type_ == kNodeColumnList, "Unexpected Index behaviour");
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

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (!ast) return DB_FAILED;
  if (current_db_.empty()) {
    std::cout << "Error: Current Database Not Assigned" << std::endl;
    return DB_FAILED;
  }

  auto target_db = dbs_[current_db_];
  std::string index_name(ast->child_->val_);

  ASSERT(!index_name.empty(), "Invalid index name");

  std::vector<IndexInfo *> all_indexes;
  std::vector<TableInfo *> all_tables;

  dberr_t res = DB_FAILED;

  if (target_db->catalog_mgr_->GetTables(all_tables) == DB_FAILED) return DB_FAILED;
  for (auto table : all_tables) {
    if (target_db->catalog_mgr_->GetTableIndexes(table->GetTableName(), all_indexes) == DB_FAILED) goto ERROR_TRAP;
    for (auto index : all_indexes) {
      if (index->GetIndexName() == index_name) {
        if (target_db->catalog_mgr_->DropIndex(table->GetTableName(), index_name) != DB_FAILED) res = DB_SUCCESS;
        goto ERROR_TRAP;
      }
      delete index;
    }
    delete table;
  }

  return DB_INDEX_NOT_FOUND;

ERROR_TRAP:
  for (auto table : all_tables) delete table;
  for (auto index : all_indexes) delete index;
  return res;
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
  return DB_FAILED;
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
