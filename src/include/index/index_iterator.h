#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
#define INVALID_ID -1

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator();

  IndexIterator(IndexIterator const &) = delete;
  void operator=(IndexIterator const &) = delete;

  IndexIterator(BufferPoolManager* _manager, const page_id_t& leaf_id);

  IndexIterator(BufferPoolManager* _manager, const page_id_t& leaf_id, const page_id_t& pair_id);

  IndexIterator(IndexIterator&& rhs);


  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
 std::shared_ptr<MappingType> data;
 BufferPoolManager* manager;
 int cur_leaf_id;
 int leaf_index;
  // add your own private member variables here
};


#endif //MINISQL_INDEX_ITERATOR_H
