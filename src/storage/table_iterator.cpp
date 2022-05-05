#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator()
    : ThisManager(nullptr), ThisPage(nullptr), ThisSchema(nullptr), ThisRow(TableIterator::INVALID_ROW) {}

TableIterator::TableIterator(BufferPoolManager *_bpm, TablePage *_tp, Schema *_s, const RowId &rid)
    : ThisManager(_bpm), ThisPage(_tp), ThisSchema(_s), ThisRow(alloc_row(rid)) {}

TableIterator::TableIterator(const TableIterator &other)
    : ThisManager(other.ThisManager),
      ThisPage(other.ThisPage),
      ThisSchema(other.ThisSchema),
      ThisRow(copy_row(other.ThisRow)) {}

TableIterator::~TableIterator() { destroy_row(ThisRow); }

bool TableIterator::operator==(const TableIterator &itr) const {
  return itr.ThisRow->GetRowId() == ThisRow->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() {
  ASSERT(ThisPage->GetTuple(ThisRow, ThisSchema, nullptr, nullptr), "TableIterator::operator* : Invalid Fetch");
  return *ThisRow;
}

Row *TableIterator::operator->() {
  ASSERT(ThisPage->GetTuple(ThisRow, ThisSchema, nullptr, nullptr), "TableIterator::operator* : Invalid Fetch");
  return ThisRow;
}

TableIterator &TableIterator::operator++() {
  auto next_rid = INVALID_ROWID;
  destroy_row(ThisRow);
  if (!ThisPage->GetNextTupleRid(ThisRow->GetRowId(), &next_rid)) {
    // this tuple is the last of this page;
    ThisPage = static_cast<TablePage *>(ThisManager->FetchPage(ThisPage->GetNextPageId()));
    if (ThisPage) {
      ThisPage->GetFirstTupleRid(&next_rid);
      ThisRow = alloc_row(next_rid);
    } else
      ThisRow = TableIterator::INVALID_ROW;

  } else //this tuple is not the last tuple
    ThisRow = alloc_row(next_rid);

  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator itr = TableIterator(*this);

  auto next_rid = INVALID_ROWID;
  destroy_row(ThisRow);
  if (!ThisPage->GetNextTupleRid(ThisRow->GetRowId(), &next_rid)) {
    // this tuple is the last of this page;
    ThisPage = static_cast<TablePage *>(ThisManager->FetchPage(ThisPage->GetNextPageId()));
    if (ThisPage) {
      ThisPage->GetFirstTupleRid(&next_rid);
      ThisRow = alloc_row(next_rid);
    } else
      ThisRow = TableIterator::INVALID_ROW;

  } else //this tuple is not the last tuple
    ThisRow = alloc_row(next_rid);

  return itr;
}
