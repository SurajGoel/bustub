//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

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
  std::cout << "Initiating cplus tree leaf page: " << page_id << " " << parent_id << " " << max_size;
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftUnderlyingArray(int start_idx, int shift_by) -> void {
  int elems = GetSize();
  int temp_array_size = elems - start_idx;
  std::pair<KeyType, ValueType> temp[temp_array_size];

  for (int i = start_idx; i < elems; i++) {
    temp[i - start_idx].first = array_[i].first;
    temp[i - start_idx].second = array_[i].second;
  }

  for (int i = start_idx; i < elems; i++) {
    array_[i + 1].first = temp[i - start_idx].first;
    array_[i + 1].second = temp[i - start_idx].second;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPairAtIndex(int index) -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PutKeyValuePairAt(int idx, const std::pair<KeyType, ValueType> &kv) -> void {
  array_[idx] = kv;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::AddKVPair(const std::pair<KeyType, ValueType> &kv) -> void {
  int idx = GetSize();
  ShiftUnderlyingArray(idx, 1);
  PutKeyValuePairAt(idx, kv);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKVPairAt(int idx, const std::pair<KeyType, ValueType> &kv) -> void {
  ShiftUnderlyingArray(idx, 1);
  PutKeyValuePairAt(idx, kv);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndexInLeafPageJustGreaterThanKey(const KeyType &key, KeyComparator &comparator)
    -> int {
  int low = 0;
  int high = GetSize() - 1;
  int result = -1;

  while (low <= high) {
    int mid = (low + high) / 2;
    int comparison = comparator(key, KeyAt(mid));

    if (comparison == 0) {
      return -1;
    }

    if (comparison == 1) {
      low = mid + 1;
    } else {
      result = mid;
      high = mid - 1;
    }
  }

  return result == -1 ? GetSize() : result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::RemoveAtIndex(int index) -> bool {
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1].first = array_[i].first;
    array_[i - 1].second = array_[i].second;
  }
  SetSize(GetSize() - 1);

  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
