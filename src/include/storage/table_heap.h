#ifndef MINISQL_TABLE_HEAP_H
#define MINISQL_TABLE_HEAP_H

#include <map>
#include <unordered_set>
#include "buffer/buffer_pool_manager.h"
#include "page/table_page.h"
#include "storage/table_iterator.h"
#include "transaction/lock_manager.h"
#include "transaction/log_manager.h"
#include <functional>
#define INSERT(C, P)                                                               \
  do {                                                                             \
    ASSERT(C[P->GetRemain()].count(P->GetTablePageId()) == 0, "Duplicate insert"); \
    C[P->GetRemain()].insert(P->GetTablePageId());                                 \
  } while (0)

#define ERASE(C, P)                                                              \
  do {                                                                           \
    ASSERT(C[P->GetRemain()].count(P->GetTablePageId()) != 0, "Invalid Delete"); \
    erase_page(P);                                \
  } while (0)
#define HAS(C, P) C[P->GetRemain()].count(P->GetTablePageId()) != 0

class TableHeap {
  friend class TableIterator;

 public:
  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new (buf) TableHeap(buffer_pool_manager, schema, txn, log_manager, lock_manager);
  }

  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new (buf) TableHeap(buffer_pool_manager, first_page_id, schema, log_manager, lock_manager);
  }

  ~TableHeap() {}

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The transaction performing the insert
   * @return true iff the insert is successful
   */
  bool InsertTuple(Row &row, Transaction *txn);

  /**
   * Mark the tuple as deleted. The actual delete will occur when ApplyDelete is called.
   * @param[in] rid Resource id of the tuple of delete
   * @param[in] txn Transaction performing the delete
   * @return true iff the delete is successful (i.e the tuple exists)
   */
  bool MarkDelete(const RowId &rid, Transaction *txn);

  /**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Transaction performing the update
   * @return true is update is successful.
   */
  bool UpdateTuple(const Row &row, const RowId &rid, Transaction *txn);

  /**
   * Called on Commit/Abort to actually delete a tuple or rollback an insert.
   * @param rid Rid of the tuple to delete
   * @param txn Transaction performing the delete.
   */
  void ApplyDelete(const RowId &rid, Transaction *txn);

  /**
   * Called on abort to rollback a delete.
   * @param[in] rid Rid of the deleted tuple.
   * @param[in] txn Transaction performing the rollback
   */
  void RollbackDelete(const RowId &rid, Transaction *txn);

  void FetchAllIds(std::unordered_set<RowId> &ans_set);

  void FetchId(std::unordered_set<RowId> &ans_set, std::size_t column_index, Schema *schema, const Field &key,
               const std::function<bool(const Field &, const Field &)> &filter);

  /**
   * Read a tuple from the table.
   * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
   * @param[in] txn transaction performing the read
   * @return true if the read was successful (i.e. the tuple exists)
   */
  bool GetTuple(Row *row, Transaction *txn);

  /**
   * Free table heap and release storage in disk file
   */
  void FreeHeap();

  /**
   * @return the begin iterator of this table
   */
  TableIterator Begin(Transaction *txn);

  /**
   * @return the end iterator of this table
   */
  TableIterator End();

  /**
   * @return the id of the first page of this table
   */
  inline page_id_t GetFirstPageId() const { return first_page_id_; }

 private:
  /**
   * create table heap and initialize first page
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn, LogManager *log_manager,
                     LockManager *lock_manager)
      : buffer_pool_manager_(buffer_pool_manager),
        schema_(schema),
        log_manager_(log_manager),
        lock_manager_(lock_manager) {
    auto FirstPage = reinterpret_cast<TablePage *>(buffer_pool_manager->NewPage(first_page_id_));
    ASSERT(FirstPage != nullptr, "TableHeap : First Page Allocation failed");
    FirstPage->Init(first_page_id_, INVALID_PAGE_ID, log_manager, txn);
    FirstPage->SetNextPageId(INVALID_PAGE_ID);
    INSERT(Pages, FirstPage);
  };

  /**
   * load existing table heap by first_page_id
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                     LogManager *log_manager, LockManager *lock_manager)
      : buffer_pool_manager_(buffer_pool_manager),
        first_page_id_(first_page_id),
        schema_(schema),
        log_manager_(log_manager),
        lock_manager_(lock_manager) {
    // This constructor is like copy constructor
    // Push every page into <Pages> for access.
    auto cur_page_id = first_page_id_;
    TablePage *cur_page = nullptr;
    while (cur_page_id != INVALID_PAGE_ID) {
      cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
      ASSERT(cur_page != nullptr, "NULL page encountered");
      INSERT(Pages, cur_page);
      cur_page_id = cur_page->GetNextPageId();
    }
  }

 private:
  BufferPoolManager *buffer_pool_manager_;
  page_id_t first_page_id_;
  Schema *schema_;

  std::map<int64_t, std::unordered_set<page_id_t>> Pages;

  void erase_page(TablePage *page) {
    auto rem = page->GetRemain();
    Pages[rem].erase(page->GetTablePageId());
    if (Pages[rem].size() == 0) Pages.erase(rem);
  }

  [[maybe_unused]] LogManager *log_manager_;
  [[maybe_unused]] LockManager *lock_manager_;
};

#endif  // MINISQL_TABLE_HEAP_H
