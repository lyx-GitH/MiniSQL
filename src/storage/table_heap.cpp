#include "storage/table_heap.h"

#define TUPLE_SIZE 8

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // too large to be stored inside.
  auto row_size = row.GetSerializedSize(schema_) + TUPLE_SIZE;
  if (row_size >= PAGE_SIZE) {
    LOG(INFO) << "Wrong !!!";
    return false;
  }
  bool isInsertSuccess = false;
  if (IsEmpty()) {
    auto FirstPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
    ASSERT(FirstPage != nullptr && first_page_id_ != INVALID_PAGE_ID,
           "TableHeap::InsertTuple Invalid Table Construction");
    FirstPage->Init(first_page_id_, INVALID_PAGE_ID, log_manager_, txn);
    FirstPage->SetNextPageId(INVALID_PAGE_ID);
    isInsertSuccess = FirstPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    INSERT(Pages, FirstPage);
    buffer_pool_manager_->FlushPage(first_page_id_);
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
  } else if (row_size > 0 - Pages.begin()->first) {
    //    LOG(INFO) << Pages.begin()->first;
    // No page is enough for insertion
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));

    ASSERT(new_page != nullptr && old_page != nullptr, "TableHeap::InsertTuple : Null While Allocating New Page");

    // set this page to the front of the list.
    new_page->Init(new_page_id, INVALID_PAGE_ID, log_manager_, txn);
    new_page->SetNextPageId(first_page_id_);
    old_page->SetPrevPageId(new_page_id);

    first_page_id_ = new_page->GetTablePageId();
    isInsertSuccess = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    ASSERT(isInsertSuccess, "invalid insert");
    INSERT(Pages, new_page);
    buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), true);
    buffer_pool_manager_->FlushPage(old_page->GetTablePageId());
    buffer_pool_manager_->UnpinPage(old_page->GetTablePageId(), true);
  } else {
    auto that_page_id = *(Pages.begin()->second.begin());
    auto that_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(that_page_id));
    ASSERT(that_page != nullptr, "TableHeap::InsertTuple : Null While Fetching Page");
    ERASE(Pages, that_page);
    isInsertSuccess = that_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    INSERT(Pages, that_page);
    buffer_pool_manager_->UnpinPage(that_page->GetTablePageId(), true);
    ASSERT(isInsertSuccess, "invalid insert");
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
  auto that_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(that_page_id));

  ASSERT(that_page != nullptr, "TableHeap::UpdateTuple : Fetching Null Tuple");
  ASSERT(HAS(Pages, that_page), "TableHeap::UpdateTuple : Fetching Null Tuple");

  ERASE(Pages, that_page);

  Row old_row_slot(rid);
  bool isUpdateSuccess = that_page->UpdateTuple(row, &old_row_slot, schema_, txn, lock_manager_, log_manager_);
  INSERT(Pages, that_page);

  buffer_pool_manager_->UnpinPage(that_page_id, isUpdateSuccess);

  return isUpdateSuccess;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));

  ASSERT(page != nullptr, "TableHeap::ApplyDelete : Page dose not exist");
  ERASE(Pages, page);
  page->ApplyDelete(rid, txn, log_manager_);
  INSERT(Pages, page);
  buffer_pool_manager_->UnpinPage(page_id, true);
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

void TableHeap::FreeHeap() {
  for (auto &page_batch : Pages) {
    for (auto &page : page_batch.second) buffer_pool_manager_->DeletePage(page);
  }
  first_page_id_ = INVALID_PAGE_ID;
  Pages.clear();
}

void TableHeap::FetchAllIds(std::unordered_set<RowId> &ans_set) {
  for (auto &page_group : Pages) {
    for (auto &page_id : page_group.second) {
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      ASSERT(page != nullptr, "Invalid Fetch");
      RowId rid;
      page->GetFirstTupleRid(&rid);
      while (!(INVALID_ROWID == rid)) {
        ans_set.insert(rid);
        page->GetNextTupleRid(rid, &rid);
      }
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
}

void TableHeap::FetchId(std::unordered_set<RowId> &ans_set, std::size_t column_index, Schema *schema, const Field &key,
                        const std::function<bool(const Field &, const Field &)> &filter) {
  for (auto &page_group : Pages) {
    for (auto &page_id : page_group.second) {
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      ASSERT(page != nullptr, "Invalid Fetch");
      RowId rid;
      page->GetFirstTupleRid(&rid);
      while (!(INVALID_ROWID == rid)) {
        Row row(rid);
        page->GetTuple(&row, schema, nullptr, nullptr);

        if (filter(*row.GetField(column_index), key)) ans_set.insert(rid);

        page->GetNextTupleRid(rid, &rid);
      }
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  if (IsEmpty()) return false;

  auto page_id = row->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));

  ASSERT(page != nullptr, "TableHeap::GetTuple : Row ID Dose Not Exist");

  bool isGet = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page_id, false);
  ASSERT(isGet, "xxx");
  return isGet;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto FirstPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (FirstPage == nullptr)
    // first page is empty
    return End();
  auto row_id = INVALID_ROWID;
  if (FirstPage->GetFirstTupleRid(&row_id)) {
    return TableIterator(buffer_pool_manager_, FirstPage, schema_, row_id);
  } else
    return End();  // First tuple is empty,
}

TableIterator TableHeap::End() { return TableIterator(buffer_pool_manager_, nullptr, schema_); }
