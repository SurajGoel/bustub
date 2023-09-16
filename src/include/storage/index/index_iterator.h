//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(BufferPoolManager *bufferPoolManager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return curr_page_id_ == itr.curr_page_id_ && curr_page_id_idx_ == itr.curr_page_id_idx_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return curr_page_id_ != itr.curr_page_id_ || curr_page_id_idx_ != itr.curr_page_id_idx_;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  page_id_t curr_page_id_;
  int curr_page_id_idx_;
};

}  // namespace bustub
