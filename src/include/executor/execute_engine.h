#ifndef MINISQL_EXECUTE_ENGINE_H
#define MINISQL_EXECUTE_ENGINE_H

#include <iomanip>
#include <string>
#include <unordered_map>
#include "common/dberr.h"
#include "common/instance.h"
#include "common/macros.h"
#include "transaction/transaction.h"

extern "C" {
#include "parser/parser.h"
};

/**DBs[db_name][table_name][index_name] = index_rows;**/

/*{*IndexName* ->{all rows}}*/
typedef std::unordered_map<std::string, std::unordered_set<std::string>> PseudoIndex;

/*{TableName, all_indexs}*/
typedef std::unordered_map<std::string, PseudoIndex> PseudoTables;

/*{DB_name, all_dbs}*/
typedef std::unordered_map<std::string, PseudoTables> PseudoDataBases;

/**
 * ExecuteContext stores all the context necessary to run in the execute engine
 * This struct is implemented by student self for necessary.
 *
 * eg: transaction info, execute result...
 */
struct ExecuteContext {
  bool flag_quit_{false};
  Transaction *txn_{nullptr};
};

enum Comparator { EQ = 0, NEQ, LT, GT, LTE, GTE };

struct Condition {
  std::string column;
  std::string operand;
  Comparator comp;
};

/**
 * ExecuteEngine
 */
class ExecuteEngine {
 public:
  ExecuteEngine();

  ~ExecuteEngine() {
    for (auto it : dbs_) {
      delete it.second;
    }
  }

  /**
   * executor interface
   */
  dberr_t Execute(pSyntaxNode ast, ExecuteContext *context);

 private:
  dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteSelect(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteInsert(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDelete(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);

 private:
  std::unordered_map<std::string, DBStorageEngine *> dbs_; /** all opened databases */
  std::string current_db_;                                 /** current database */
  static PseudoDataBases database_structure;

  static bool generate_db_struct(const std::string &db_name, const DBStorageEngine *db);

  void make_db_tuple(pSyntaxNode head, const Schema &schema, std::unordered_map<std::string, std::size_t> &column_index,
                     std::vector<Field> &data_tup);

  bool check_index_constrains(const std::string &table_name, const std::vector<Field> &data_tuple,
                              std::unordered_map<std::string, std::size_t> &column_index);

  void update_index(const std::string &table_name, const RowId &rid, const std::vector<Field> &data_tuple,
                    std::unordered_map<std::string, std::size_t> &column_index, bool insert = true);

  void update_index(const std::string &table_name, const RowId &rid, const std::vector<Field *> &data_tuple,
                    std::unordered_map<std::string, std::size_t> &column_index, bool insert = true);

  bool parse_column_definitions(const std::string &table_name, pSyntaxNode head);
  Column *parse_single_column(pSyntaxNode ast, const int table_position, bool is_nullable, bool is_unique);

  bool parse_condition(pSyntaxNode ast, const TableInfo *table_info, std::unordered_set<RowId> &ans_set);

  bool parse_compare(pSyntaxNode ast, const TableInfo *table_info, std::unordered_set<RowId> &ans_set);

  IndexInfo *find_index(const TableInfo *table_info, const std::string &column_name);

  Field get_field(pSyntaxNode ast, const TableInfo *table_info);

  void pretty_print(TableInfo *table_info, std::vector<std::string> &used_columns,
                    std::unordered_map<std::string, std::size_t> &column_index, std::unordered_set<RowId> &ans_set);

  void do_update(const TableInfo* table_info, map<string, Field> new_values, unordered_set<RowId> effected_rows,
                 unordered_map<string, size_t> column_index);
};

#endif  // MINISQL_EXECUTE_ENGINE_H
