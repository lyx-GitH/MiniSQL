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

  IndexIterator(BufferPoolManager* _bpm, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* _p);

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
 BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* ThisBPage;
 int index;
 BufferPoolManager* ThisManager;
 MappingType* ThisPair;
  // add your own private member variables here
};


#endif //MINISQL_INDEX_ITERATOR_H
