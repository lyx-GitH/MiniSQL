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
  //  LOG(INFO) << "Internal_init. "
  //            << "id: " << page_id << " parent: " << parent_id << std::endl;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  if(index <0 || index >= GetSize()) {
    LOG(ERROR) << "Invalid Index: "<<index<<" "<<GetSize()<<" "<<GetPageId();
  }
  ASSERT(index >= 0 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt : Invalid Index");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  ASSERT(index >= 0 && index < BPlusTreePage::GetSize(), "B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt : Invalid Index");
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < BPlusTreePage::GetSize(); i++){
//    std::cout<<"here: "<<array_[i].second<<" "<<value<<std::endl;
    if (value == array_[i].second) return i;
  }
//  std::cout << "NP"<<std::endl;
  throw std::out_of_range("Value Index Not Found");
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
  auto pos = BinarySearchNode(key, comparator);
  if (pos >= GetSize())
    return array_[GetSize() - 1].second;
  else if (comparator(array_[pos].first, key) == 0)
    return array_[pos].second;
  else
    return array_[pos - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUpIndex(const KeyType &key, const KeyComparator &comparator) const {
  auto pos = BinarySearchNode(key, comparator);
  if (pos >= GetSize())
    return GetSize()-1;
  else if (comparator(array_[pos].first, key) == 0)
    return pos;
  else
    return pos-1;
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
  array_[0] = std::make_pair(KeyType{}, old_value);
  array_[1] = std::make_pair(new_key, new_value);
  SetParentPageId(INVALID_PAGE_ID);
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
  for (; i < GetSize(); i++)
    if (old_value == array_[i].second) break;
  for (int j = GetSize(); j > i + 1; j--) {
    array_[j] = array_[j - 1];
  }
  array_[i + 1] = std::make_pair(new_key, new_value);
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
  recipient->SetParentPageId(GetParentPageId());
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
  ASSERT(index >=0 && index < GetSize(),"B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove : Invalid Index" );
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
//  LOG(INFO)<<"update root";
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
/*
 * ["rec*]   [this]  ===> [*rec* | this]
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  ASSERT(this != recipient, "Self Copy");
  array_[0].first = middle_key;
  for(int i=0; i<GetSize(); i++)
    recipient->CopyLastFrom(array_[i], buffer_pool_manager);
  SetSize(0);
}

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
                                                      BufferPoolManager *buffer_pool_manager) {
  ASSERT(this != recipient, "Self Copy");
  auto first_pair = std::make_pair(middle_key, array_[0].second);
  recipient->CopyLastFrom(first_pair, buffer_pool_manager);

  for(int i=1; i<GetSize(); i++)
    array_[i-1] = array_[i];

  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    IncreaseSize(1);
    array_[GetSize()-1] = pair;
    auto child_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(pair.second)->GetData());
    ASSERT(child_page != nullptr, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom : Null Fetch");
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(pair.second, true);
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
                                                       BufferPoolManager *buffer_pool_manager) {
  ASSERT(this != recipient, "Self Copy");
  ASSERT(GetSize() > 0, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf : Empty Page");
  recipient->SetKeyAt(0, middle_key);
  auto& last_pair = array_[GetSize() -1];
  recipient->CopyFirstFrom(last_pair, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    IncreaseSize(1);
    for(int i = GetSize(); i>=1; i--) {
      array_[i] = array_[i-1];
    }

    array_[0] = pair;
    auto child_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(pair.second)->GetData());
    ASSERT(child_page != nullptr, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom : Null Fetch");
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(array_[0].second, true);
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::BinarySearchNode(const KeyType &key, const KeyComparator &comparator) const {
  if (GetSize() <= 1)
    return 0;

  //TODO: Here may cause bugs
  int left = 1;//0
  int right = GetSize()- 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    auto &mid_data = array_[mid].first;
    if (comparator(mid_data,key) == 0)
      return mid;
    else if (comparator(mid_data, key) < 0)
      left = mid + 1;
    else
      right = mid - 1;
  }

  return left;
}

template class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;