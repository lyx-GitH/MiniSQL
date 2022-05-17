#include "index/index_iterator.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator()
    : data(nullptr), manager(nullptr), cur_leaf_id(INVALID_PAGE_ID), leaf_index(INVALID_PAGE_ID) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *_manager, const page_id_t &leaf_id)
    : manager(_manager), cur_leaf_id(leaf_id) {
  auto leaf =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(manager->FetchPage(leaf_id)->GetData());
  if (leaf == nullptr) {
    cur_leaf_id = INVALID_PAGE_ID;
    leaf_index = INVALID_PAGE_ID;
  } else {
    leaf_index = 0;
    data = new MappingType();
    *data = leaf->GetItem(leaf_index);
    manager->UnpinPage(cur_leaf_id, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if (data != nullptr) delete data;
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() { return *data; }

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      manager->FetchPage(cur_leaf_id)->GetData());
  ASSERT(leaf_page != nullptr, "NULL");
  if (leaf_index + 1 < leaf_page->GetSize()) {
    ++leaf_index;
    *data = leaf_page->GetItem(leaf_index);
  } else {
    cur_leaf_id = leaf_page->GetNextPageId();
    if (cur_leaf_id == INVALID_PAGE_ID) {
      if (data != nullptr) delete data;
      leaf_index = INVALID_PAGE_ID;
      return *this;
    }
    auto next_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
        manager->FetchPage(cur_leaf_id)->GetData());
    ASSERT(next_leaf != nullptr, "NULL");
    leaf_index = 0;
    *data = next_leaf->GetItem(leaf_index);
    manager->UnpinPage(cur_leaf_id, false);
  }
  manager->UnpinPage(leaf_page->GetPageId(), false);

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return itr.cur_leaf_id == cur_leaf_id && itr.leaf_index == leaf_index;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(*this == itr); }

template class IndexIterator<int, int, BasicComparator<int>>;

template class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
