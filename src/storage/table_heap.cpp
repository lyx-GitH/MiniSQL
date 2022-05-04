#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // too large to be stored inside.
  auto row_size = row.GetSerializedSize(schema_);
  if (row_size >= PAGE_SIZE) return false;
  bool isInsertSuccess = false;
  if (row_size > Pages.begin()->first) {
    // No page is enough for insertion
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));

    ASSERT(new_page != nullptr, "TableHeap::InsertTuple : Null While Allocating New Page");

    // set this page to the front of the list.
    new_page->Init(new_page_id, INVALID_PAGE_ID, log_manager_, txn);
    new_page->SetNextPageId(first_page_id_);
    first_page_id_ = new_page->GetTablePageId();
    isInsertSuccess = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    INSERT(Pages, new_page);
  } else {
    auto that_page_id = *(Pages.begin()->second.begin());
    auto that_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(that_page_id));
    ASSERT(that_page != nullptr, "TableHeap::InsertTuple : Null While Fetching Page");
    ERASE(Pages, that_page);
    isInsertSuccess = that_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    INSERT(Pages, that_page);
  }

  return isInsertSuccess;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }

  ERASE(Pages, page);
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  INSERT(Pages, page);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  if (row.GetSerializedSize(schema_) > PAGE_SIZE) return false;

  auto that_page_id = rid.GetPageId();
  auto that_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(that_page_id));

  ASSERT(that_page != nullptr, "TableHeap::UpdateTuple : Fetching Null Tuple");
  ASSERT(HAS(Pages, that_page), "TableHeap::UpdateTuple : Fetching Null Tuple");

  ERASE(Pages, that_page);

  Row old_row_slot(rid);
  bool isUpdateSuccess = that_page->UpdateTuple(row, &old_row_slot, schema_, txn, lock_manager_, log_manager_);
  INSERT(Pages, that_page);

  return isUpdateSuccess;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page_id = rid.GetPageId();
  auto page = static_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));

  ASSERT(page != nullptr, "TableHeap::ApplyDelete : Page dose not exist");
  ERASE(Pages, page);
  page->ApplyDelete(rid, txn, log_manager_);
  INSERT(Pages, page);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  ERASE(Pages, page);
  page->RollbackDelete(rid, txn, log_manager_);
  INSERT(Pages, page);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page_id = row->GetRowId().GetPageId();
  auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));

  ASSERT(page != nullptr, "TableHeap::GetTuple : Row ID Dose Not Exist");


  return page->GetTuple(row, schema_, txn, lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  ASSERT(first_page_id_ != INVALID_PAGE_ID, "TableHeap::Begin : Empty TableHeap");
  auto FirstPage = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  auto FirstTupleId = INVALID_ROWID;
  ASSERT(FirstPage->GetFirstTupleRid(&FirstTupleId), "TableHeap::Begin: Empty First Tuple");
  return TableIterator(buffer_pool_manager_, FirstPage, schema_, FirstTupleId);
}

TableIterator TableHeap::End() { return TableIterator(buffer_pool_manager_, nullptr, schema_, INVALID_ROWID); }
