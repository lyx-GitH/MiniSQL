#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() : ThisManager(nullptr), ThisPage(nullptr), ThisSchema(nullptr) {}

TableIterator::TableIterator(BufferPoolManager *_bpm, TablePage* _tp, Schema *_s, const RowId &rid) {
  ThisManager = _bpm;
  ThisSchema = _s;
  ThisPage = _tp;
  ThisRowID = rid;
}



TableIterator::TableIterator(const TableIterator &other) {
  ThisManager = other.ThisManager;
  ThisPage = other.ThisPage;
  ThisSchema = other.ThisSchema;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return ThisManager == itr.ThisManager && ThisRowID == itr.ThisRowID;
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() {
  Row* row_out = new Row(ThisRowID);
  ASSERT(ThisPage->GetTuple(row_out, ThisSchema, nullptr, nullptr), "TableIterator::operator* : Invalid Fetch");
  return *row_out;
}

Row *TableIterator::operator->() {
  Row* row_out = new Row(ThisRowID);
  ASSERT(ThisPage->GetTuple(row_out, ThisSchema, nullptr, nullptr), "TableIterator::operator* : Invalid Fetch");
  return row_out;

}

TableIterator &TableIterator::operator++() {
  if(! ThisPage->GetNextTupleRid(ThisRowID, &ThisRowID)){
    //This row is the last row
    ThisPage = static_cast<TablePage *>(ThisManager->FetchPage(ThisPage->GetNextPageId()));
    if(ThisPage) {
      ThisPage->GetFirstTupleRid(&ThisRowID);
    } else ThisRowID = INVALID_ROWID;
  }
  //else , just go to the next row

  return *this;
}

TableIterator TableIterator::operator++(int) {
  TablePage* _ThisPage = ThisPage;
  RowId _ThisRowID = ThisRowID;
  if(! ThisPage->GetNextTupleRid(ThisRowID, &_ThisRowID)){
    //This row is the last row
    _ThisPage = static_cast<TablePage *>(ThisManager->FetchPage(ThisPage->GetNextPageId()));
    if(_ThisPage) {
      _ThisPage->GetFirstTupleRid(&_ThisRowID);
    } else _ThisRowID = INVALID_ROWID;
  }

  return TableIterator(ThisManager, ThisPage, ThisSchema, ThisRowID);
}
