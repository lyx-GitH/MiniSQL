#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"


class TableHeap;
class TablePage;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(BufferPoolManager* _bpm,TablePage* _tp, Schema* _s, const RowId& rid);

  explicit TableIterator(BufferPoolManager* _bpm, TablePage* tp, Schema* s, RowId* r);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here

 BufferPoolManager* ThisManager;
 TablePage* ThisPage;
 Schema* ThisSchema;
 RowId ThisRowID = INVALID_ROWID;


};

#endif //MINISQL_TABLE_ITERATOR_H
