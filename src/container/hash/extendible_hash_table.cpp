//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) {
  std::cout << "Hello !!";
  bucket_size_ = bucket_size;
  dir_.resize(2);
  global_depth_ = 1;
  auto bucket1 = std::make_shared<Bucket>(bucket_size, 1);
  auto bucket2 = std::make_shared<Bucket>(bucket_size, 1);
  dir_[0] = bucket1;
  dir_[1] = bucket2;
  num_buckets_ = 2;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::IncrementGlobalDepthInternal() {
  global_depth_++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> lock(this->latch_);
  size_t dir_index = this->IndexOf(key);
  return dir_.at(dir_index)->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> lock(this->latch_);
  size_t dir_index = this->IndexOf(key);
  return dir_.at(dir_index)->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(this->latch_);
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  size_t dir_index = this->IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_.at(dir_index);

  if (!bucket->IsFull()) {
    bucket->Insert(key, value);
    return;
  }

  V new_val;
  if (bucket->Find(key, new_val)) {
    bucket->Insert(key, value);
    return;
  }

  if (bucket->GetDepth() == this->GetGlobalDepthInternal()) {
    size_t prev_size = dir_.size();
    dir_.resize(dir_.size() * 2);
    for (size_t i = 0; i < prev_size; i++) {
      dir_[i + prev_size] = dir_[i];
    }
    IncrementGlobalDepthInternal();
  }

  SplitBucket(bucket, dir_index);
  this->InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(std::shared_ptr<Bucket> bucket, size_t dir_index) {
  K &first_item = bucket->GetFirstItem();
  size_t cur_key_hash = std::hash<K>()(first_item);
  auto incremented_depth_bit_mask = 1 << (bucket->GetDepth());
  auto cur_key_hash_incremented_depth_bit = cur_key_hash & incremented_depth_bit_mask;
  std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);

  std::list<K> removal_list;
  for (std::pair<K, V> &item : bucket->GetItems()) {
    // Arrange elements separately according to their incremented depth bit
    if ((std::hash<K>()(item.first) & incremented_depth_bit_mask) != cur_key_hash_incremented_depth_bit) {
      new_bucket->Insert(item.first, item.second);
      removal_list.push_back(item.first);
    }
  }

  for (auto remove_k : removal_list) {
    bucket->Remove(remove_k);
  }

  // Now see what are all the indexes in directory where this bucket was being pointed at
  // Find a little smarter way to do this !!! And you will good to go then !!!
  for (int i = 0; i <= (1 << (GetGlobalDepthInternal() - bucket->GetDepth())) - 1; i++) {
    size_t idx = (i == 0) ? (dir_index & ((1 << bucket->GetDepth()) - 1))
                          : ((i << bucket->GetDepth()) | (dir_index & ((1 << bucket->GetDepth()) - 1)));

    if ((idx & incremented_depth_bit_mask) == cur_key_hash_incremented_depth_bit) {
      dir_[idx] = bucket;
    } else {
      dir_[idx] = new_bucket;
    }
  }

  bucket->IncrementDepth();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const std::pair<K, V> &entry : list_) {
    if (entry.first == key) {
      value = entry.second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto entry = list_.begin(); entry != list_.end(); entry++) {
    if (entry->first == key) {
      list_.erase(entry);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (this->IsFull()) {
    return false;
  }

  // Check if the value already exists.
  for (auto &entry : list_) {
    if (entry.first == key) {
      entry.second = value;
      return true;
    }
  }

  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
