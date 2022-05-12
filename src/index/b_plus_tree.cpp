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
  if (page == nullptr){
    LOG(ERROR) <<"Null Page";
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
  bool isLeaf = IS_LEAF(node);
  auto new_page_id = INVALID_PAGE_ID;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr){
    LOG(ERROR)<<"Split: Null Page";
    throw std::bad_alloc();
  }
  auto r_page = reinterpret_cast<N *>(new_page);
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
//  auto middle_key = new_node->IsLeafPage() ? static_cast<LeafPage *>(new_node)->KeyAt(0)
//                                           : static_cast<InternalPage *>(new_node)->KeyAt(0);

  if (parent_id == INVALID_PAGE_ID ||old_node->GetPageId() == root_page_id_) {
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
      buffer_pool_manager_->UnpinPage(target_int_node->GetPageId(), true);
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
  auto target_leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  target_leaf->RemoveAndDeleteRecord(key, comparator_);
  //  if (size_after < 0 || size_after >= target_leaf->GetMinSize())
  //    return;

  bool isDelete = CoalesceOrRedistribute(target_leaf, transaction);

  buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), isDelete);
  if (isDelete) buffer_pool_manager_->DeletePage(target_leaf->GetPageId());
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
  if (node->GetSize() >= node->GetMinSize()) return false;
  if (!IS_LEAF(node) && node->IsRootPage()) {
    // node is root and node is dead, i.e, root is empty
    ASSERT(node->GetSize() == 1, "BPLUSTREE_TYPE::CoalesceOrRedistribute : Invalid Root Victim");

    auto root_page = TO_TYPE(InternalPage *, node);
    auto new_root_page = TO_TYPE(BPlusTreePage *, buffer_pool_manager_->FetchPage(root_page->ValueAt(0))->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_page->GetPageId();
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }

  N *sib = nullptr;
  InternalPage *parent = nullptr;
  int p_index = -1;
  AssignBrother(node, sib, parent, p_index);
  //  auto middle_key = parent->ValueAt(p_index);
  // if p_index = 0, sib is right brother. else, sib is left_brother
  if (node->GetSize() <= node->GetMinSize() && sib->GetSize() <= sib->GetMinSize())
    return Coalesce(node, sib, parent, p_index);
  else {
    Redistribute(node, sib, p_index);
    buffer_pool_manager_->UnpinPage(sib->GetPageId(), true);
    return false;
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::AssignBrother(N *&left, N *&right, InternalPage *&parent, int &index) {
  auto parent_id = TO_TYPE(BPlusTreePage *, left)->GetParentPageId();
  parent = TO_TYPE(InternalPage *, buffer_pool_manager_->FetchPage(parent_id)->GetData());
  index = parent->ValueIndex(TO_TYPE(BPlusTreePage *, left)->GetPageId());
  if (index == 0) {
    right = TO_TYPE(N *, buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1))->GetData());
  } else {
    right = TO_TYPE(N *, buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());
  }
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
  bool kill_node = false;
  if (IS_LEAF(node)) {
    if (index == 0) {
      // sib is right
      TO_TYPE(LeafPage *, sib)->MoveAllTo(TO_TYPE(LeafPage *, node));
      buffer_pool_manager_->UnpinPage(sib->GetPageId(), false);
      buffer_pool_manager_->DeletePage(sib->GetPageId());
      parent->Remove(1);
    } else {
      TO_TYPE(LeafPage *, node)->MoveAllTo(TO_TYPE(LeafPage *, sib));
      kill_node = true;
      buffer_pool_manager_->UnpinPage(sib->GetPageId(), true);
      parent->Remove(index);
    }
  } else {
    if (index == 0) {
      TO_TYPE(InternalPage *, sib)->MoveAllTo(TO_TYPE(InternalPage *, node), parent->KeyAt(1), buffer_pool_manager_);
      buffer_pool_manager_->UnpinPage(sib->GetPageId(), false);
      buffer_pool_manager_->DeletePage(sib->GetPageId());
      parent->Remove(1);
    } else {
      TO_TYPE(InternalPage *, node)
          ->MoveAllTo(TO_TYPE(InternalPage *, sib), parent->KeyAt(index), buffer_pool_manager_);
      kill_node = true;
      buffer_pool_manager_->UnpinPage(sib->GetPageId(), true);
      parent->Remove(index);
    }
  }
  bool kill_parent = CoalesceOrRedistribute(parent, transaction);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), !kill_parent);
  if (kill_parent) buffer_pool_manager_->DeletePage(parent->GetPageId());

  return kill_node;
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
  // index = 0, sib is the right sib
  ASSERT(node->GetParentPageId() == sib->GetParentPageId(), "BPLUSTREE_TYPE::Redistribute : Not Sibling");
  auto parent_page = TO_TYPE(InternalPage *, buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  auto middle_key = index == 0 ? parent_page->KeyAt(1) : parent_page->KeyAt(index);
  if (IS_LEAF(node)) {
    if (index == 0) {
      TO_TYPE(LeafPage *, sib)->MoveFirstToEndOf(TO_TYPE(LeafPage *, node));
      parent_page->SetKeyAt(1, sib->KeyAt(0));
    } else {
      TO_TYPE(LeafPage *, sib)->MoveLastToFrontOf(TO_TYPE(LeafPage *, node));
      parent_page->SetKeyAt(index, node->KeyAt(0));
    }
  } else {
    if (index == 0) {
      // sib is the right sib
      TO_TYPE(InternalPage *, sib)->MoveFirstToEndOf(TO_TYPE(InternalPage *, node), middle_key, buffer_pool_manager_);
      parent_page->SetKeyAt(1, node->KeyAt(0));
    } else {
      // sib is the left sib
      TO_TYPE(InternalPage *, sib)->MoveLastToFrontOf(TO_TYPE(InternalPage *, node), middle_key, buffer_pool_manager_);
      parent_page->SetKeyAt(index, node->KeyAt(0));
    };
  }
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
  if (IsEmpty()) return nullptr;
  auto cur_page_id = root_page_id_;
  auto cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
  BPlusTreePage *cur_b_page = nullptr;

  InternalPage *cur_internal_page = nullptr;

  ASSERT(cur_page != nullptr, "BPLUSTREE_TYPE::GetValue : Invalid Page");

  while (true) {
    cur_b_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    if (cur_b_page->IsLeafPage()) break;
    cur_internal_page = reinterpret_cast<InternalPage *>(cur_page->GetData());
    auto value = leftMost ? cur_internal_page->ValueAt(0) : cur_internal_page->Lookup(key, comparator_);
    cur_page_id = *(reinterpret_cast<page_id_t *>(&value));
    buffer_pool_manager_->UnpinPage(cur_b_page->GetPageId(), false);
    cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
  }

  ASSERT(cur_page != nullptr && cur_b_page != nullptr && cur_b_page->IsLeafPage(),
         "BPLUSTREE_TYPE::GetValue : Invalid Page : Leaf Unreached");
  //  target_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(cur_page->GetData());
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

template class BPlusTree<int, int, BasicComparator<int>>;

template class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
