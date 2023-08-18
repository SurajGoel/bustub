#include <algorithm>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {

  auto curr_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  while (!curr_page->IsLeafPage()) {
    auto curr_page_as_internal = reinterpret_cast<InternalPage *>(curr_page);
    int i = 1;

    // Break when key in internal page (curr_key) is greater than the new key we are trying to find
    for (i = 1; i < curr_page_as_internal->GetSize(); i++) {
      auto curr_key = curr_page_as_internal->KeyAt(i);
      if (comparator_(curr_key, key) == 1) {
        break;
      }
    }

    auto next_page_id = curr_page_as_internal->ValueAt(i-1);
    curr_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }

  // Now we have got the leaf page where the value should be
  auto curr_page_as_leaf = reinterpret_cast<LeafPage *>(curr_page);
  for (int i = 0; i < curr_page_as_leaf->GetSize(); i++) {
    auto curr_key = curr_page_as_leaf->KeyAt(i);
    if (comparator_(key, curr_key) == 0) {
      result->push_back(curr_page_as_leaf->ValueAt(i));
      return true;
    }
  }

  return false;
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {

  if (root_page_id_ == INVALID_PAGE_ID) {
    auto new_root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    new_root_page->SetPageType(IndexPageType::LEAF_PAGE);
    new_root_page->SetIsRootPage(true);
  }

  auto curr_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  while (!curr_page->IsLeafPage()) {
    auto curr_page_as_internal = reinterpret_cast<InternalPage *>(curr_page);

    // Convert the below iteration to binary search
    int i = 1;
    for (i = 1; i < curr_page_as_internal->GetSize(); i++) {
      auto curr_key = curr_page_as_internal->KeyAt(i);

      // Break when key in internal page (curr_key) is greater than the new key we are trying to insert
      if (comparator_(curr_key, key) == 1) {
        break;
      }
    }
    auto next_page_id = curr_page_as_internal->ValueAt(i-1);

    curr_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }

  // Now the current page is the leaf page !!
  auto curr_page_as_leaf = reinterpret_cast<LeafPage *>(curr_page);

  if (curr_page_as_leaf->GetSize() == leaf_max_size_) {
    return SplitLeafPageAndAddKey(curr_page_as_leaf, key, value);
  }

  int i = curr_page_as_leaf->FindIndexInLeafPageJustGreaterThanKey(key, comparator_);
  if (i == -1) {
    return false;
  }

  curr_page_as_leaf->InsertKVPairAt(i, std::make_pair(key, value));
  return true;
}

// internal Page => 1 , 3
// key => 4
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // Find the element in the leaf page, then delete it

  auto curr_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  while (!curr_page->IsLeafPage()) {
    auto curr_page_as_internal = reinterpret_cast<InternalPage *>(curr_page);
    int i = 1;

    // Break when key in internal page (curr_key) is greater than the new key we are trying to find
    for (i = 1; i < curr_page_as_internal->GetSize(); i++) {
      auto curr_key = curr_page_as_internal->KeyAt(i);
      if (comparator_(curr_key, key) == 1) {
        break;
      }
    }

    auto next_page_id = curr_page_as_internal->ValueAt(i-1);
    curr_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }

  // Now we have got the leaf page where the value should be
  auto curr_page_as_leaf = reinterpret_cast<LeafPage *>(curr_page);
  for (int i = 0; i < curr_page_as_leaf->GetSize(); i++) {
    auto curr_key = curr_page_as_leaf->KeyAt(i);
    if (comparator_(key, curr_key) == 0) {
      curr_page_as_leaf->RemoveAtIndex(i);

      if (curr_page_as_leaf->GetSize() <= (curr_page_as_leaf->GetMaxSize() / 2)) {
        CoalesceLeafNode(curr_page_as_leaf);
      }

      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::CoalesceLeafNode(BPlusTree::LeafPage *leafPage) -> bool {

  if (leafPage->GetSize() > leafPage->GetMaxSize() / 2) {
    return false;
  }

  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leafPage->GetParentPageId())->GetData());

  page_id_t right_leaf_page_id = parent_page->FindNextPageId(leafPage->GetPageId());
  auto right_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(right_leaf_page_id)->GetData());
  if (right_leaf_page->GetSize() + leafPage->GetSize() < leaf_max_size_) {
    MergeLeafNode(right_leaf_page, leafPage);
    RemoveFromParentPage(right_leaf_page);
    return true;
  }

  page_id_t left_leaf_page_id = parent_page->FindPreviousPageId(leafPage->GetPageId());
  auto left_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(left_leaf_page_id)->GetData());
  if (left_leaf_page->GetSize() + leafPage->GetSize() < leaf_max_size_) {
    MergeLeafNode(leafPage, left_leaf_page);
    RemoveFromParentPage(leafPage);
    return true;
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::CoalesceInternalNode(BPlusTree::InternalPage *internalPage) -> bool {

  if (internalPage->GetSize() >= internalPage->GetMaxSize() / 2) {
    return false;
  }

  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internalPage->GetParentPageId())->GetData());

  page_id_t right_page_id = parent_page->FindNextPageId(internalPage->GetPageId());
  auto right_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(right_page_id)->GetData());

  if (right_page->GetSize() + internalPage->GetSize() < internal_max_size_) {
    MergeInternalNode(right_page, internalPage);
    return RemoveFromParentPage(right_page);
  }

  page_id_t left_page_id = parent_page->FindPreviousPageId(internalPage->GetPageId());
  auto left_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left_page_id)->GetData());
  if (left_page->GetSize() + internalPage->GetSize() < internal_max_size_) {
    MergeInternalNode(internalPage, left_page);
    return RemoveFromParentPage(internalPage);
  }

  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoInternalPage(InternalPage *internalPage, const KeyType &key, const page_id_t &value) -> bool {

  if (internalPage->GetSize() == internal_max_size_) {
    // You can't afford to insert -> which means you will have to split this internal page itself first,
    // otherwise just find the index where this key should be inserted and insert it there

    // In similar way to leaf page split this and insert into the parent internal page
    page_id_t new_internal_page_id;
    auto new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_page_id)->GetData());
    new_internal_page->Init(new_internal_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_internal_page->SetIsRootPage(false);

    int index_to_insert = internalPage->FindIndexInInternalPageJustGreaterThanKey(key, comparator_);
    if(index_to_insert == -1) {
      return false;
    }

    int mid = internalPage->GetSize() / 2;
    if (index_to_insert > mid) {
      mid++;
    }

    int count_being_moved = internalPage->GetSize() - mid;
    for (int i = mid; i < internalPage->GetSize(); i++) {
      page_id_t childPageId = internalPage->ValueAt(i);
      new_internal_page->PutKeyValuePairAt(i-mid, std::make_pair(internalPage->KeyAt(i), childPageId));

      auto childPage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(childPageId)->GetData());
      childPage->SetParentPageId(new_internal_page_id);
    }

    internalPage->SetSize(mid);
    internalPage->SetIsRootPage(false);
    new_internal_page->SetSize(count_being_moved);

    (index_to_insert < internal_max_size_ &&
     index_to_insert <= mid) ?
        InsertAndUpdateParentPage(internalPage, index_to_insert, key, value) :
        InsertAndUpdateParentPage(new_internal_page, index_to_insert-mid, key, value);

    // This seems okay !!! -> Now insert the new leaf page into the parent of leafPage which is an internal page
    int parent_page_id = internalPage->GetParentPageId();
    InternalPage *parent_page = nullptr;
    if (parent_page_id == INVALID_PAGE_ID) {
      int new_parent_page_id;
      auto new_parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_parent_page_id)->GetData());
      new_parent_page->Init(new_parent_page_id, INVALID_PAGE_ID, internal_max_size_);
      new_parent_page->SetIsRootPage(true);
      root_page_id_ = new_parent_page_id;
      UpdateRootPageId();
      parent_page = new_parent_page;
      parent_page_id = new_parent_page_id;
      InsertIntoInternalPage(parent_page, internalPage->KeyAt(0), internalPage->GetPageId());
      internalPage->SetParentPageId(parent_page_id);
    } else {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    }

    InsertIntoInternalPage(parent_page, new_internal_page->KeyAt(0), new_internal_page_id);
    new_internal_page->SetParentPageId(parent_page_id);

    return true;

  }

  int index_to_insert = internalPage->FindIndexInInternalPageJustGreaterThanKey(key, comparator_);
  if(index_to_insert == -1) {
    return false;
  }

  // This is not greater than internal max size, so we can insert in here
  InsertAndUpdateParentPage(internalPage, index_to_insert, key, value);
//  internalPage->InsertKVPairAt(index_to_insert, std::make_pair(key, value));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPageAndAddKey(BPlusTree::LeafPage *leafPage, const KeyType &key, const ValueType &value) -> page_id_t {

  int index_to_insert = leafPage->FindIndexInLeafPageJustGreaterThanKey(key, comparator_);
  if (index_to_insert == -1) {
    return -1;
  }

  page_id_t new_leaf_page_id;
  auto new_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_leaf_page_id)->GetData());
  new_leaf_page->Init(new_leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);
  new_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
  new_leaf_page->SetIsRootPage(false);

  int mid = leafPage->GetSize() / 2;
  if (index_to_insert > mid) {
    mid++;
  }

  int count_being_moved = leafPage->GetSize() - mid;
  for (int i = mid; i < leafPage->GetSize(); i++) {
    new_leaf_page->PutKeyValuePairAt(i-mid, std::make_pair(leafPage->KeyAt(i), leafPage->ValueAt(i)));
  }

  leafPage->SetSize(mid);
  leafPage->SetNextPageId(new_leaf_page_id);
  leafPage->SetIsRootPage(false);
  new_leaf_page->SetSize(count_being_moved);

  (index_to_insert < leaf_max_size_ && index_to_insert <= mid) ? leafPage->InsertKVPairAt(index_to_insert, std::make_pair(key, value))
                         : new_leaf_page->InsertKVPairAt(index_to_insert - mid, std::make_pair(key, value));

  // This seems okay !!! -> Now insert the new leaf page into the parent of leafPage which is an internal page
  int parent_page_id = leafPage->GetParentPageId();
  InternalPage *parent_page = nullptr;
  if (parent_page_id == INVALID_PAGE_ID) {
    int new_parent_page_id;
    auto new_parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_parent_page_id)->GetData());
    new_parent_page->Init(new_parent_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_parent_page->SetIsRootPage(true);
    root_page_id_ = new_parent_page_id;
    UpdateRootPageId();
    parent_page = new_parent_page;
    parent_page_id = new_parent_page_id;
    InsertIntoInternalPage(parent_page, leafPage->KeyAt(0), leafPage->GetPageId());
  } else {
    parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  }

  InsertIntoInternalPage(parent_page, new_leaf_page->KeyAt(0), new_leaf_page_id);
  // Parent Page updation should be happening inside this only !!
//  leafPage->SetParentPageId(parent_page_id);
//  new_leaf_page->SetParentPageId(parent_page_id);

  return new_leaf_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPageAndAddKey(BPlusTree::LeafPage *leafPage, const KeyType &key, const ValueType &value) -> page_id_t {

  int index_to_insert = leafPage->FindIndexInLeafPageJustGreaterThanKey(key, comparator_);
  if (index_to_insert == -1) {
    return -1;
  }

  page_id_t new_leaf_page_id;
  auto new_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_leaf_page_id)->GetData());
  new_leaf_page->Init(new_leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);
  new_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
  new_leaf_page->SetIsRootPage(false);

  int mid = leafPage->GetSize() / 2;
  if (index_to_insert > mid) {
    mid++;
  }

  int count_being_moved = leafPage->GetSize() - mid;
  for (int i = mid; i < leafPage->GetSize(); i++) {
    new_leaf_page->PutKeyValuePairAt(i-mid, std::make_pair(leafPage->KeyAt(i), leafPage->ValueAt(i)));
  }

  leafPage->SetSize(mid);
  leafPage->SetNextPageId(new_leaf_page_id);
  leafPage->SetIsRootPage(false);
  new_leaf_page->SetSize(count_being_moved);

  index_to_insert <= mid ? leafPage->InsertKVPairAt(index_to_insert, std::make_pair(key, value))
                         : new_leaf_page->InsertKVPairAt(index_to_insert - mid, std::make_pair(key, value));

  // This seems okay !!! -> Now insert the new leaf page into the parent of leafPage which is an internal page
  int parent_page_id = leafPage->GetParentPageId();
  InternalPage *parent_page = nullptr;
  if (parent_page_id == INVALID_PAGE_ID) {
    int new_parent_page_id;
    auto new_parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_parent_page_id)->GetData());
    new_parent_page->Init(new_parent_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_parent_page->SetIsRootPage(true);
    root_page_id_ = new_parent_page_id;
    UpdateRootPageId();
    parent_page = new_parent_page;
    parent_page_id = new_parent_page_id;
    InsertIntoInternalPage(parent_page, leafPage->KeyAt(0), leafPage->GetPageId());
  } else {
    parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  }

  InsertIntoInternalPage(parent_page, new_leaf_page->KeyAt(0), new_leaf_page_id);
  leafPage->SetParentPageId(parent_page_id);
  new_leaf_page->SetParentPageId(parent_page_id);

  return new_leaf_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertAndUpdateParentPage(BPlusTree::InternalPage *insertTo,
                                               int idx,
                                               const KeyType &key,
                                               const page_id_t &value) {

  insertTo->InsertKVPairAt(idx, std::make_pair(key, value));
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(value)->GetData());
  child_page->SetParentPageId(insertTo->GetPageId());

  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeafNode(BPlusTree::LeafPage *fromLeafPage,
                                   BPlusTree::LeafPage *toLeafPage) -> bool {

  for (int i=0 ; i<fromLeafPage->GetSize() ; i++) {
    toLeafPage->AddKVPair(std::make_pair(fromLeafPage->KeyAt(i), fromLeafPage->ValueAt(i)));
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternalNode(BPlusTree::InternalPage *fromPage,
                                       BPlusTree::InternalPage *toPage) -> bool {

  for (int i=0 ; i<fromPage->GetSize() ; i++) {
    toPage->AddKVPair(std::make_pair(fromPage->KeyAt(i), fromPage->ValueAt(i)));
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveFromParentPage(BPlusTreePage *bPlusTreePage) -> bool {

  page_id_t parent_page_id = bPlusTreePage->GetParentPageId();
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

  if (parent_page->RemovePageId(bPlusTreePage->GetPageId())) {
    return CoalesceInternalNode(parent_page);
  }

  return false;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
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
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
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
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
