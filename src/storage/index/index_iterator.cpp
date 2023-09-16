/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bufferPoolManager) { bpm_ = bufferPoolManager; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (curr_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto curr_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(curr_page_id_)->GetData());
  if (!curr_page->IsLeafPage()) {
    return true;
  }

  auto curr_page_as_leaf = reinterpret_cast<LeafPage *>(curr_page);
  return static_cast<bool>(curr_page_id_idx_ == curr_page_as_leaf->GetSize() - 1 &&
                           curr_page_as_leaf->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto curr_page = reinterpret_cast<LeafPage *>(bpm_->FetchPage(curr_page_id_)->GetData());
  return curr_page->GetPairAtIndex(curr_page_id_idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }

  auto curr_page = reinterpret_cast<LeafPage *>(bpm_->FetchPage(curr_page_id_)->GetData());
  if (curr_page_id_idx_ == curr_page->GetSize() - 1) {
    curr_page_id_idx_ = 0;
    curr_page_id_ = curr_page->GetNextPageId();
  } else {
    curr_page_id_idx_++;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
