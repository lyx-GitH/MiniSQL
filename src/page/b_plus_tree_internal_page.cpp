#include "page/b_plus_tree_internal_page.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

// THE FIRST KEY IS ALWAYS INVALID

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageType(IndexPageType::INTERNAL_PAGE);
  BPlusTreePage::SetSize(0);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  ASSERT(index >= 0 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt : Invalid Index");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  ASSERT(index >= 1 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt : Invalid Index");
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int i = 0;
  for (; i < BPlusTreePage::GetSize(); i++)
    if (value == array_[i].second) break;
  return i == BPlusTreePage::GetSize() ? INVALID_PAGE_ID : i;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  ASSERT(index >= 0 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt : Invalid Index");
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code

  auto index = BinarySearch(key, comparator);
  return array_[index].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  BPlusTreePage::SetSize(2);
  BPlusTreePage::SetPageId(INVALID_PAGE_ID);
  array_[0] = std::make_pair(KeyType{}, old_value);
  array_[1] = std::make_pair(new_key, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int i = 0;
  for (; i < BPlusTreePage::GetSize(); i++)
    if (array_[i].second == old_value) break;

  if (i == BPlusTreePage::GetSize()) return i;

  for (int j = BPlusTreePage::GetSize(); j > i; j--) array_[j] = array_[j - 1];

  array_[i] = make_pair(new_key, new_value);
  BPlusTreePage::IncreaseSize(1);
  return BPlusTreePage::GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  auto half_index = BPlusTreePage::GetSize() >> 1;
  for (int i = half_index; i < BPlusTreePage::GetSize(); ++i) {
    recipient->CopyLastFrom(array_[i], buffer_pool_manager);
  }
  SetSize(half_index);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  const auto parent_page_id = BPlusTreePage::GetPageId();
  BPlusTreePage::SetSize(size);
  for (int i = 0; i < size; i++) {
    array_[i] = items[i];
    auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(items[i].second)->GetData());
    ASSERT(page != nullptr, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom : Null Page");
    page->SetParentPageId(parent_page_id);
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < BPlusTreePage::GetSize() - 1; i++) array_[i] = array_[i + 1];
  BPlusTreePage::IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  // ASSERT(BPlusTreePage::GetSize() == 1, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild : Multiple
  // Children");
  BPlusTreePage::SetSize(0);
  return array_[0].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto page = GetBPlusNode(buffer_pool_manager, pair.second);
  ASSERT(page != nullptr, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom : Invalid Page");
  page->SetParentPageId(GetPageId());
  array_[BPlusTreePage::GetSize()] = pair;
  BPlusTreePage::IncreaseSize(1);
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::BinarySearch(const KeyType &key, const KeyComparator &comparator) const {
  int low = 1, high = BPlusTreePage::GetSize() - 1;
  if (comparator(key, array_[high].first) > 0) {
    // key is larger than the max element
    return high + 1;
  }
  while (high > low) {
    int mid = (low + high) >> 1;
    if (comparator(key, array_[mid].first) == 0) return mid;
    if (comparator(key, array_[mid].first) > 0)
      low = mid + 1;
    else
      high = mid - 1;
  }

  auto index = (comparator(key, array_[low].first) > 0) ? (low + 1) : low;
  return index - 1;
}

template class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;