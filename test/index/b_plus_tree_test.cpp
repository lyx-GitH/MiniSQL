#include "index/b_plus_tree.h"
#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

// TEST(BPlusTreeTests, RemoveTest) {
//   DBStorageEngine engine(db_name);
//   BasicComparator<int> comparator;
//   BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//   TreeFileManagers mbr("tree_");
//
//   vector<int>  push;
//   const int n = 60;
////  for(int i=0; i<)
//}

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 17, 17);
  TreeFileManagers mbr("tree_");
  // Prepare data
  const int n = 600;
  const int removed = n >> 1;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }

  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }

  tree.PrintTree(std::cout);

  ASSERT_TRUE(tree.Check());
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());

  // Delete half keys

  // Print tree

  for (int i = 0; i < removed; i++) {
    tree.Remove(delete_seq[i]);
  }


  // Check valid
  ans.clear();
  for (int i = 0; i < removed; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = removed; i < n; i++) {
    //    LOG(INFO)<<"Getting: "<<delete_seq[i];
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}