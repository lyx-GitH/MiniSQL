#include "index/b_plus_tree.h"
#include <string>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/index_roots_page.h"

#define TO_TYPE(Type, ptr) reinterpret_cast<Type>(ptr)
#define IS_LEAF(N) reinterpret_cast<BPlusTreePage *>(N)->IsLeafPage()

INDEX_TEMPLATE_ARGUMENTS
std::list<page_id_t> BPLUSTREE_TYPE::deleted_pages;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy(BPlusTreePage *node) {
  if (node->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(node->GetPageId());
    return;
  } else {
    BPlusTreePage *target;
    for (int i = 0; i < node->GetSize(); i++) {
      auto next_page_id = reinterpret_cast<InternalPage *>(node)->ValueAt(i);
      target = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
      Destroy(target);
      buffer_pool_manager_->UnpinPage(next_page_id, false);
      buffer_pool_manager_->DeletePage(next_page_id);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  if (IsEmpty()) return;
  auto root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  Destroy(root_page);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  if (IsEmpty()) return false;
  auto target_page = FindLeafPage(key, false);
  ASSERT(target_page, "BPLUSTREE_TYPE::GetValue : Empty Target Leaf");
  auto target_leaf = reinterpret_cast<LeafPage *>(target_page);
  ASSERT(target_leaf->IsLeafPage(), "BPLUSTREE_TYPE::GetValue : Not A Leaf");
  auto value = ValueType{};
  bool isFindSucceed = target_leaf->Lookup(key, value, comparator_);
  if (isFindSucceed) result.push_back(value);
  buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), false);
  return isFindSucceed;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page_id = INVALID_PAGE_ID;
  auto page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    LOG(ERROR) << "Null Page";
    throw std::bad_alloc();
  }

  root_page_id_ = page_id;
  auto RootPage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());

  RootPage->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  RootPage->SetPageType(IndexPageType::LEAF_PAGE);

  RootPage->Insert(key, value, comparator_);
  UpdateRootPageId(true);
  buffer_pool_manager_->UnpinPage(RootPage->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto target_page = FindLeafPage(key);
  ASSERT(target_page != nullptr, "BPLUSTREE_TYPE::InsertIntoLeaf : Unable To Find Leaf");
  auto target_leaf = reinterpret_cast<LeafPage *>(target_page->GetData());
  ASSERT(target_leaf->IsLeafPage(), "BPLUSTREE_TYPE::InsertIntoLeaf : target leaf is not a leaf");
  auto size = target_leaf->Insert(key, value, comparator_);

  if (size < 0) {
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), false);
    return false;
  }

  if (size <= target_leaf->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), true);
    return true;
  } else {
    /*
     * {-Left- | Right}  ==>  {-Left-}->{Right}
     */
    //    LOG(INFO)<<"To BE Split: "<<target_leaf->GetPageId() <<" "<<target_leaf->GetParentPageId();
    LeafPage *r_page = Split(target_leaf);
    ASSERT(r_page->IsLeafPage(), "r_page is not a leaf");
    auto middle_key = r_page->KeyAt(0);
    InsertIntoParent(target_leaf, middle_key, r_page, transaction);
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(r_page->GetPageId(), true);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  //  LOG(INFO) << "node's parent and id" <<node->GetParentPageId()<<" "<<node->GetPageId()<<std::endl;
  bool isLeaf = IS_LEAF(node);
  auto new_page_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    LOG(ERROR) << "Split: Null Page";
    throw std::bad_alloc();
  }
  auto r_page = reinterpret_cast<N *>(new_page);
  //  LOG(INFO) << "Split Assign";
  r_page->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  r_page->SetPageType(isLeaf ? IndexPageType::LEAF_PAGE : IndexPageType::INTERNAL_PAGE);
  if (isLeaf)
    TO_TYPE(LeafPage *, node)->MoveHalfTo(TO_TYPE(LeafPage *, r_page));
  else
    TO_TYPE(InternalPage *, node)->MoveHalfTo(TO_TYPE(InternalPage *, r_page), buffer_pool_manager_);
  return r_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &middle_key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  ASSERT(old_node->GetParentPageId() == new_node->GetParentPageId(),
         "BPLUSTREE_TYPE::InsertIntoParent : Not Same Parent");
  const auto parent_id = old_node->GetParentPageId();


  if (parent_id == INVALID_PAGE_ID || old_node->GetPageId() == root_page_id_) {
    // No parent. i.e. Root is Split.
    auto new_root_id = INVALID_PAGE_ID;
    auto root_page_raw = buffer_pool_manager_->NewPage(new_root_id);
    if (root_page_raw == nullptr) throw std::bad_alloc();

    auto new_root_page = reinterpret_cast<InternalPage *>(root_page_raw->GetData());

    new_root_page->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), middle_key, new_node->GetPageId());
    root_page_id_ = new_root_page->GetPageId();

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
  } else {
    auto page = buffer_pool_manager_->FetchPage(parent_id);
    ASSERT(page != nullptr, "BPLUSTREE_TYPE::InsertIntoParent : Invalid Parent");
    auto target_int_node = reinterpret_cast<InternalPage *>(page->GetData());
    ASSERT(!target_int_node->IsLeafPage(), "BPLUSTREE_TYPE::InsertIntoParent : Not a leaf page");
    target_int_node->InsertNodeAfter(old_node->GetPageId(), middle_key, new_node->GetPageId());
    if (target_int_node->GetSize() > target_int_node->GetMaxSize()) {
      // Split internal
      InternalPage *r_page = Split(target_int_node);
      auto new_middle_key = r_page->KeyAt(0);
      InsertIntoParent(target_int_node, new_middle_key, r_page);
      buffer_pool_manager_->UnpinPage(r_page->GetPageId(), true);
    }
    // No Split
    buffer_pool_manager_->UnpinPage(target_int_node->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;

  deleted_pages.clear();
  auto target_leaf = TO_TYPE(LeafPage *, FindLeafPage(key)->GetData());
  ASSERT(target_leaf != nullptr && target_leaf->IsLeafPage(), "Remove: Unqualified leaf");

  target_leaf->RemoveAndDeleteRecord(key, comparator_);
  auto rm_leaf = CoalesceOrRedistribute(target_leaf, transaction);
  buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), true);

  if (rm_leaf) {
    ASSERT(target_leaf->GetSize() == 0, "Delete Not Empty Page");
    deleted_pages.push_front(target_leaf->GetPageId());
  }
  /*TODO:
   * Here, if I delete *ANY* page, the root page is set to NULL.
   * This needs to be FIXED.
   */
  // TODO: When this bug is fixed, Uncomment these codes to safe delete all the to-be-deleted nodes.
  //   bool root_is_dead = false;
  //   for (auto id : deleted_pages) {
  //      buffer_pool_manager_->DeletePage(id);
  //   }
  //   deleted_pages.clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // this node is okay
  if (node->GetSize() >= node->GetMinSize()) return false;

  if (node->GetParentPageId() == INVALID_PAGE_ID) {
    ASSERT(node->GetPageId() == root_page_id_ && node->GetSize() == 1, "Unqualified root");
    std::cout << "Update Root" << std::endl;
    if (node->IsLeafPage() == false) {
      root_page_id_ = TO_TYPE(InternalPage *, node)->RemoveAndReturnOnlyChild();
      auto new_root_page = TO_TYPE(BPlusTreePage *, buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

      ASSERT(new_root_page->GetParentPageId() == node->GetPageId(), "Not the child");
      ASSERT(new_root_page->GetPageId() == root_page_id_, "Unqualified root");
      new_root_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(root_page_id_, false);
      return true;
    } else
      return false;
  }

  N *sib = nullptr;
  InternalPage *parent = nullptr;
  int p_index = -1;
  AssignBrother(node, sib, parent, p_index);
  ASSERT(sib != nullptr && parent != nullptr && p_index >= 0 && p_index < parent->GetSize(),
         "Invalid Brother Assignment");

  if (sib->GetSize() > sib->GetMinSize()) {
    Redistribute(node, sib, p_index);
    return false;
  } else
    return Coalesce(node, sib, parent, p_index, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::AssignBrother(N *left, N *&right, InternalPage *&parent, int &index) {
  ASSERT(left != nullptr && left->GetSize() < left->GetMinSize(), "NULL left page");
  auto parent_id = left->GetParentPageId();
  ASSERT(parent_id != INVALID_PAGE_ID, "No Brother");
  parent = TO_TYPE(InternalPage *, buffer_pool_manager_->FetchPage(parent_id)->GetData());
  ASSERT(parent != nullptr && parent->IsLeafPage() == false, "NULL parent");
  index = parent->ValueIndex(left->GetPageId());

  ASSERT(index >= 0 && index < parent->GetSize(), "Invalid Index");

  if (index == 0)
    right = TO_TYPE(N *, buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1))->GetData());
  else
    right = TO_TYPE(N *, buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());

  ASSERT(right != nullptr && right->GetParentPageId() == left->GetParentPageId(), "Invalid Assignment");
  ASSERT(right != left && (void *)right != (void *)parent && (void *)left != (void *)parent, "Self Assignment");
}
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *node, N *sib, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                              int index, Transaction *transaction) {
  // node is the less leaf.
  //std::cout << "Col" << node->GetPageId() << " " << sib->GetPageId() << " " << parent->GetPageId() << std::endl;
  bool rm_node = false;

  ASSERT(parent != nullptr, "Emoty Parent");
  ASSERT(parent->ValueAt(index) == node->GetPageId() || parent->ValueAt(index) == sib->GetPageId(),
         "BPLUSTREE_TYPE::Coalesce : MiddleKey Error");
  ASSERT(node->IsLeafPage() == sib->IsLeafPage(), "Not the same type");
  ASSERT(node->GetParentPageId() == sib->GetParentPageId(), "Not Same Parent");

  if (node->IsLeafPage()) {
    if (index == 0)  //[node]->[sib] ====> [*node* | sib]
    {
      std::cout << "Left Col" << std::endl;
      TO_TYPE(LeafPage *, sib)->MoveAllTo(TO_TYPE(LeafPage *, node));
      TO_TYPE(LeafPage *, node)->SetNextPageId(TO_TYPE(LeafPage *, sib)->GetNextPageId());
      parent->Remove(1);
      ASSERT(parent->ValueAt(0) == node->GetPageId(), "FAIL");
    } else  //[sib]->[node] =====> [*sib* | node]
    {
      TO_TYPE(LeafPage *, node)->MoveAllTo(TO_TYPE(LeafPage *, sib));
      rm_node = true;
      TO_TYPE(LeafPage *, sib)->SetNextPageId(TO_TYPE(LeafPage *, node)->GetNextPageId());
      TO_TYPE(LeafPage *, sib)->SetParentPageId(node->GetParentPageId());
      parent->Remove(index);
    }
  } else {
    if (index == 0) {
      TO_TYPE(InternalPage *, sib)->MoveAllTo(TO_TYPE(InternalPage *, node), parent->KeyAt(1), buffer_pool_manager_);
      parent->Remove(1);
    } else {
      TO_TYPE(InternalPage *, node)
          ->MoveAllTo(TO_TYPE(InternalPage *, sib), parent->KeyAt(index), buffer_pool_manager_);
      rm_node = true;
      parent->Remove(index);
      ASSERT(parent->ValueAt(index - 1) == sib->GetPageId(), "FAILED");
      ASSERT(node->GetSize() == 0, "xxx");
    }
  }

  auto rm_parent = CoalesceOrRedistribute(parent, transaction);

  buffer_pool_manager_->UnpinPage(sib->GetPageId(), rm_node);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), !rm_parent);

  if (rm_parent) {
    ASSERT(parent->GetSize() == 0, "Delete not null page");
    deleted_pages.push_back(parent->GetPageId());
  }
  if (!rm_node) {
    ASSERT(sib->GetSize() == 0, "Delete not null page");
    deleted_pages.push_back(sib->GetPageId());
  }

  return rm_node;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *node, N *sib, int index) {
  //std::cout << "Re" << std::endl;
  // node is the less leaf
  //  index = 0, sib is the right sib
  ASSERT(node->GetParentPageId() == sib->GetParentPageId(), "BPLUSTREE_TYPE::Redistribute : Not Sibling");
  auto parent_page = TO_TYPE(InternalPage *, buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());

  ASSERT(parent_page != nullptr, "Redistribute : Invalid Parent Page");

  if (node->IsLeafPage()) {
    if (index == 0) {
      //[node] -> [sib]
      ASSERT(parent_page->ValueAt(1) == sib->GetPageId(), "xxx");
      TO_TYPE(LeafPage *, sib)->MoveFirstToEndOf(TO_TYPE(LeafPage *, node));
      parent_page->SetKeyAt(1, sib->KeyAt(0));
    } else {
      //[sib]->[node]
      ASSERT(parent_page->ValueAt(index) == node->GetPageId(), "xxx");
      TO_TYPE(LeafPage *, sib)->MoveLastToFrontOf(TO_TYPE(LeafPage *, node));
      parent_page->SetKeyAt(index, node->KeyAt(0));
    }
  } else {
    if (index == 0) {
      //[node] -> [sib]
      ASSERT(parent_page->ValueAt(1) == sib->GetPageId(), "xxx");
      TO_TYPE(InternalPage *, sib)
          ->MoveFirstToEndOf(TO_TYPE(InternalPage *, node), parent_page->KeyAt(1), buffer_pool_manager_);
      parent_page->SetKeyAt(1, sib->KeyAt(0));
    } else {
      ASSERT(parent_page->ValueAt(index) == node->GetPageId(), "xxx");
      TO_TYPE(InternalPage *, sib)
          ->MoveLastToFrontOf(TO_TYPE(InternalPage *, node), parent_page->KeyAt(index), buffer_pool_manager_);
      parent_page->SetKeyAt(index, node->KeyAt(0));
    }
  }
  // update target

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return true; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //  vector<int> traces{};
  if (IsEmpty()) return nullptr;
  auto cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto cur_id = root_page_id_;
  //  traces.push_back(root_page_id_);
  while (cur_page != nullptr && TO_TYPE(BPlusTreePage *, cur_page->GetData())->IsLeafPage() == false) {
    cur_id = cur_page->GetPageId();
    auto target_int_page = TO_TYPE(InternalPage *, cur_page->GetData());
    ASSERT(target_int_page->IsLeafPage() == false, "Not A InternalPage");
    auto next_index = leftMost ? target_int_page->ValueAt(0) : target_int_page->Lookup(key, comparator_);
    //    traces.push_back(next_index);
    buffer_pool_manager_->UnpinPage(target_int_page->GetPageId(), false);
    cur_page = buffer_pool_manager_->FetchPage(next_index);
  }
  //
  if (cur_page == nullptr) {
    LOG(ERROR) << "null fecth: " << cur_id;
  }
  ASSERT(cur_page != nullptr, "Invalid Leaf Page");
  return cur_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto page = buffer_pool_manager_->FetchPage(0);
  ASSERT(page != nullptr, "BPLUSTREE_TYPE::UpdateRootPageId : Invalid Root Index Id");
  auto index_page = reinterpret_cast<IndexRootsPage *>(page->GetData());
  if (insert_record) {
    ASSERT(index_page->Insert(index_id_, root_page_id_), "BPLUSTREE_TYPE::UpdateRootPageId : Insert Failed");
  } else {
    ASSERT(index_page->Update(index_id_, root_page_id_), "BPLUSTREE_TYPE::UpdateRootPageId : Update Failed");
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ostream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::printOut(bool All) {
  vector<int> path_ids;

  auto cur_page = TO_TYPE(BPlusTreePage *, buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  if (!cur_page) {
    std::cout << "NULL ROOT " << root_page_id_ << std::endl;
    return;
  }
  while (cur_page != nullptr && cur_page->IsLeafPage() == false) {
    path_ids.push_back(cur_page->GetPageId());
    auto new_id = TO_TYPE(InternalPage *, cur_page)->ValueAt(0);
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    cur_page = TO_TYPE(BPlusTreePage *, buffer_pool_manager_->FetchPage(new_id)->GetData());
  }
  path_ids.push_back(cur_page->GetPageId());

  if (cur_page == nullptr || cur_page->IsLeafPage() == false) {
    std::cout << " Path Error" << std::endl;
  } else
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

  std::deque<int> dq;
  dq.push_back(root_page_id_);
  auto cur_layer = 0;

  while (!dq.empty()) {
    auto page = buffer_pool_manager_->FetchPage(dq.front());
    if (!page) {
      std::cout << "ERROR: nil node at " << dq.front() << std::endl;
      return;
    }
    dq.pop_front();
    if (page->GetPageId() == path_ids[cur_layer]) {
      ++cur_layer;
      std::cout << std::endl;
    }
    if (IS_LEAF(page->GetData())) {
      printNode(TO_TYPE(LeafPage *, page->GetData()), All);
    } else {
      auto ip = TO_TYPE(InternalPage *, page->GetData());
      printNode(ip, All);
      for (int i = 0; i < ip->GetSize(); i++) dq.push_back(ip->ValueAt(i));
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }

  std::cout << std::endl;
}

template class BPlusTree<int, int, BasicComparator<int>>;

template class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
