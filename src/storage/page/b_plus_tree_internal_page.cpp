//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
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
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  auto existing_pair = array_[index];
  existing_pair.first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftUnderlyingArray(int start_idx, int shift_by) -> void {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndexInInternalPageJustGreaterThanKey(const KeyType &key,
                                                                               KeyComparator &comparator) -> int {
  int low = 1;
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::AddKVPair(const std::pair<KeyType, ValueType> &kv) -> void {
  int idx = GetSize();
  ShiftUnderlyingArray(idx, 1);
  PutKeyValuePairAt(idx, kv);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKVPairAt(int idx, const std::pair<KeyType, ValueType> &kv) -> void {
  ShiftUnderlyingArray(idx, 1);
  PutKeyValuePairAt(idx, kv);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PutKeyValuePairAt(int idx, const std::pair<KeyType, ValueType> &kv) -> void {
  array_[idx] = kv;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::RemoveAtIndex(int index) -> bool {
  if (index < 0 || index >= GetSize()) {
    return false;
  }

  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1].first = array_[i].first;
    array_[i - 1].second = array_[i].second;
  }
  SetSize(GetSize() - 1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindNextPageId(page_id_t curr_page_id) -> page_id_t {
  int next_page_id = INVALID_PAGE_ID;
  for (int idx = 0; idx < GetSize(); idx++) {
    if (array_[idx].second == curr_page_id) {
      if (idx <= GetSize() - 1) {
        next_page_id = array_[idx + 1].second;
        break;
      }
    }
  }

  return next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindPreviousPageId(page_id_t curr_page_id) -> page_id_t {
  int prev_page_id = INVALID_PAGE_ID;
  for (int idx = 0; idx < GetSize(); idx++) {
    if (array_[idx].second == curr_page_id) {
      if (idx > 0) {
        prev_page_id = array_[idx - 1].second;
        break;
      }
    }
  }

  return prev_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemovePageId(page_id_t curr_page_id) -> bool {
  // From the internal page, find the curr page id and the corresponding index, and then call RemoveAtIndex with that
  // index as the argument
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == curr_page_id) {
      return RemoveAtIndex(i);
    }
  }

  return false;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

}  // namespace bustub
