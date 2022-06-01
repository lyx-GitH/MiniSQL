#include "page/b_plus_tree_leaf_page.h"
#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageType(IndexPageType::LEAF_PAGE);
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetMaxSize(max_size);
  BPlusTreePage::SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
  //  LOG(INFO) <<"Leaf Init. "<<"id: "<<page_id<<" parent: "<<parent_id<<std::endl;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  return BinarySearch(key, comparator);
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  //  KeyType key{};
  //  return key;
  //  ASSERT(index >= 0 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt : Invalid Index");
  if (index < 0 && index > GetSize()) {
    LOG(ERROR) << "index: " << index << " "
               << "Size: " << GetSize();
  }

  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  ASSERT(index >= 0 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem : Invalid Index");
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  auto size = BPlusTreePage::GetSize();
  if (!size) {
    array_[0] = {key, value};
    BPlusTreePage::IncreaseSize(1);
    return GetSize();
  }

  auto insert_place = BinarySearch(key, comparator);
  //  ASSERT(insert_place > GetSize() || comparator(key, array_[insert_place].first) <= 0, "Wrong Insert Place");
  if (insert_place < GetSize() && comparator(array_[insert_place].first, key) == 0) return -1;
  for (int i = size - 1; i >= insert_place; i--) array_[i + 1] = array_[i];
  array_[insert_place] = MappingType(key, value);
  IncreaseSize(1);
  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  auto half_index = GetSize() >> 1;
  for (int i = half_index; i < BPlusTreePage::GetSize(); ++i) {
    recipient->CopyLastFrom(array_[i]);
  }
  recipient->next_page_id_ = next_page_id_;
  next_page_id_ = recipient->GetPageId();
  BPlusTreePage::SetSize(half_index);
  recipient->SetParentPageId(GetParentPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = 0; i < size; i++) {
    CopyLastFrom(items[i]);
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  auto insert_place = BinarySearch(key, comparator);
  if (insert_place < GetSize() && comparator(key, array_[insert_place].first) == 0)  // got that key
  {
    value = array_[insert_place].second;
    return true;
  } else
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  auto del_pos = BinarySearch(key, comparator);
  if (del_pos < GetSize() && array_[del_pos].first == key) {
    for (int i = del_pos; i < GetSize() - 1; i++) {
      array_[i] = array_[i + 1];
    }

    IncreaseSize(-1);
    return GetSize();
  } else
    return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 * [Re]->[This]->Next   ---- [Re|This]->Next
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  ASSERT(this != recipient, "Self Copy");
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) recipient->CopyLastFrom(array_[i]);
  //  recipient->SetNextPageId(next_page_id_);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  ASSERT(this != recipient, "Self Copy");
  recipient->CopyLastFrom(array_[0]);
  for (int i = 0; i < BPlusTreePage::GetSize() - 1; i++) array_[i] = array_[i + 1];
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_[BPlusTreePage::GetSize()] = item;
  BPlusTreePage::IncreaseSize(1);  // add one to the size.
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  ASSERT(this != recipient, "Self Copy");
  auto last_key = array_[BPlusTreePage::GetSize() - 1];
  recipient->CopyFirstFrom(last_key);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  const auto size = GetSize();

  for (int i = size - 1; i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  array_[0] = item;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::BinarySearch(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0, right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) >> 1;
    if (comparator(array_[mid].first, key) > 0)
      right = mid - 1;
    else if (comparator(array_[mid].first, key) < 0)
      left = mid + 1;
    else
      return mid;
  }
  return right + 1;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::FetchValues(const KeyType &key, bool left, bool key_included,
                                                                       unordered_set<ValueType> &ans_set,
                                                                       const KeyComparator &comparator) {
  auto key_index = BinarySearch(key, comparator);
  // key index is the first value that v >= key
  if (left) {
    if (key_included && key_index < GetSize() && comparator(key, array_[key_index].first) == 0)
      ans_set.insert(array_[key_index].second);
    for (int i = 0; i < key_index; i++) ans_set.insert(array_[i].second);
  } else {
    if (key_included && comparator(key, array_[key_index].first) == 0 && key_index < GetSize())
      ans_set.insert(array_[key_index].second);
    if (key_index < GetSize() && !key_included && comparator(array_[key_index].first, key) > 0)
      ans_set.insert(array_[key_index].second);
    for (int i = key_index + 1; i < GetSize(); i++) ans_set.insert(array_[i].second);
  }
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::FetchAllValues(unordered_set<ValueType> &ans_set) {
  for (int i = 0; i < GetSize(); i++) ans_set.insert(array_[i].second);
}

template class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;