#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "buffer/buffer_pool_manager.h"
#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include <map>

class TableHeap;
class TablePage;

class TableIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(BufferPoolManager *_bpm, TablePage *_tp, Schema *_s, const RowId &rid);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

  static Row* INVALID_ROW;

 private:
  // add your own private member variables here

  static std::map<Row *, uint32_t> ptr_refs;



  BufferPoolManager *ThisManager;
  TablePage *ThisPage;
  Schema *ThisSchema;
  Row *ThisRow = nullptr;

  Row *alloc_row(const RowId &rid) {
    Row *row = new Row(rid);
    ptr_refs.insert(std::make_pair(row, 1));
    return row;
  }

  void destroy_row(Row *ptr) {
    if (ptr == nullptr || ptr == INVALID_ROW) return;

    ASSERT(ptr_refs.count(ptr) != 0, "TableIterator::destroy_row: Invalid ptr");
    --ptr_refs[ptr];
    if (ptr_refs[ptr] == 0) {
      delete ptr;
      ptr_refs.erase(ptr);
    }
  }

  Row *copy_row(Row *ptr) {
    if (ptr && ptr != INVALID_ROW) {
      ASSERT(ptr_refs.count(ptr) != 0, "TableIterator::copy_row: Invalid ptr");
      ++ptr_refs[ptr];
    }

    return ptr;
  }
};






#endif  // MINISQL_TABLE_ITERATOR_H
