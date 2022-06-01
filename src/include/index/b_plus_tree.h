#ifndef MINISQL_B_PLUS_TREE_H
#define MINISQL_B_PLUS_TREE_H

#include <queue>
#include <string>
#include <vector>

#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_page.h"
#include "queue"
#include "transaction/transaction.h"

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
class CatalogMeta;

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  static std::list<page_id_t> deleted_pages;

  explicit BPlusTree(index_id_t index_id,page_id_t root_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  inline page_id_t GetRootPageId(){return root_page_id_;};

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  void RangeScan(const KeyType& key, std::unordered_set<ValueType>& ans_set, bool to_left, bool key_included);


  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction = nullptr);

  bool GetValue(const KeyType& key, std::unordered_set<ValueType>& ans_set);

  INDEXITERATOR_TYPE Begin();

  INDEXITERATOR_TYPE Begin(const KeyType &key);

  INDEXITERATOR_TYPE End();

  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

  // used to check whether all pages are unpinned
  bool Check();

  // destroy the b plus tree
  void Destroy();


  void Destroy(BPlusTreePage *node);

  void OutputTree() {
    if (IsEmpty()) return;
    Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(root_page);
    ToString(node, buffer_pool_manager_);
  }

  void PrintTree(std::ostream &out) {
    if (IsEmpty()) {
      return;
    }
    out << "digraph G {" << std::endl;
    Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(root_page);
    ToGraph(node, buffer_pool_manager_, out);
    out << "}" << std::endl;
  }

  void printOut(bool All = false) ;

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N>
  N *Split(N *node);

  template <typename N>
  void AssignBrother(N *left, N *&right, InternalPage *&parent, int &index);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                Transaction *transaction = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ostream &out) const;

  template <typename N>
  void printNode(N *node, bool All = false) {
    if (!node) {
      std::cout << "(NULL NODE)";
      return;
    }

    auto size = node->GetSize();
    if (All) {
      if (node->IsLeafPage()) {
        std::cout << '{';
        for (int i = 0; i < size - 1; i++) std::cout << node->KeyAt(i) << ", ";
        std::cout << node->KeyAt(size - 1) << "}";
      } else {
        std::cout << '[';
        for (int i = 0; i < size - 1; i++) {
          if (i == 0) {
            std::cout << '*' << node->KeyAt(0) << '*' << ", ";
          } else
            std::cout << node->KeyAt(i) << ", ";
        }
        std::cout << node->KeyAt(size - 1) << ']';
      }
    } else {
      if (node->IsLeafPage()) {
        std::cout << '{' << node->GetParentPageId() << '|';
        for (int i = 0; i < size - 1; i++)
          std::cout << "X"
                    << ", ";
        std::cout << "X" << '|' << node->GetPageId() << "}";
      } else {
        std::cout << '[' << node->GetParentPageId() << '|';
        for (int i = 0; i < size - 1; i++) {
          std::cout << (reinterpret_cast<InternalPage *>(node))->ValueAt(i) << ", ";
        }
        std::cout << (reinterpret_cast<InternalPage *>(node))->ValueAt(size - 1) << '|' << node->GetPageId() << ']';
      }
    }
  }

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  index_id_t index_id_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

#endif  // MINISQL_B_PLUS_TREE_H
