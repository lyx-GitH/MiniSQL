#include "index/index_iterator.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator()
    : ThisBPage(nullptr), index(INVALID_PAGE_ID), ThisManager(nullptr), ThisPair(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *_bpm, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *_p)
    : ThisBPage(_p), index(0), ThisManager(_bpm) {
  ThisPair = new MappingType(ThisBPage->GetItem(index));
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if(ThisBPage != nullptr)
    ThisManager->UnpinPage(ThisBPage->GetPageId(), true);
  if(ThisPair != nullptr)
    delete ThisBPage;
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return *ThisPair;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(index + 1 < ThisBPage->GetSize())
    ++index;
  else if(ThisBPage->GetNextPageId() == INVALID_PAGE_ID){
    //End Leaf
    ThisManager->UnpinPage(ThisBPage->GetPageId(), true);
    ThisBPage = nullptr;
  }


  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return itr.ThisBPage->GetPageId() == ThisBPage->GetPageId() && index == itr.index;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}

template class IndexIterator<int, int, BasicComparator<int>>;

template class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
