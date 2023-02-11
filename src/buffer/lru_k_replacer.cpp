//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(num_frames) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_history_set_.empty()) {
    return false;
  }

  auto frame_ptr = frame_history_set_.rbegin();
  *frame_id = frame_ptr->get()->GetFrameId();
  // This is the frame that should be removed
  RemoveFrameFromSetInternal(frame_ptr->get()->GetFrameId());
  frame_index_map_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto frame = frame_index_map_.find(frame_id);

  if (frame == frame_index_map_.end()) {
    auto new_frame = std::make_shared<FrameAccessHistory>(frame_id, k_);
    new_frame->RecordAccess();
    frame_index_map_.insert({frame_id, new_frame});
    frame_history_set_.insert(new_frame);
  } else {
    RemoveFrameFromSetInternal(frame_id);
    frame->second->RecordAccess();
    if (frame->second->IsEvictable()) {
      frame_history_set_.insert(frame->second);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto frame = frame_index_map_.find(frame_id)->second;
  RemoveFrameFromSetInternal(frame_id);
  frame->SetEvictable(set_evictable);
  if (set_evictable && frame_history_set_.size() < replacer_size_) {
    frame_history_set_.insert(frame);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto frame = frame_index_map_.find(frame_id);

  if (frame == frame_index_map_.end()) {
    return;
  }

  if (frame->second->IsEvictable()) {
    RemoveFrameFromSetInternal(frame_id);
    frame_index_map_.erase(frame_id);
    return;
  }

  exit(1);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return frame_history_set_.size();
}
auto LRUKReplacer::RemoveFrameFromSetInternal(frame_id_t frame_id) -> void {
  auto find_frame = [frame_id](const std::shared_ptr<FrameAccessHistory> &fr) { return fr->GetFrameId() == frame_id; };
  auto found_frame = std::find_if(frame_history_set_.begin(), frame_history_set_.end(), find_frame);
  if (found_frame != frame_history_set_.end()) {
    frame_history_set_.erase(found_frame);
  }
}

//===--------------------------------------------------------------------===//
// FrameAccessHistory
//===--------------------------------------------------------------------===//

void LRUKReplacer::FrameAccessHistory::RecordAccess() {
  std::lock_guard<std::mutex> lock(frame_latch_);

  auto duration = std::chrono::system_clock::now().time_since_epoch();
  auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  // TODO(sgoel): Take lock and perform
  size_t history_size = access_history_.size();
  if (history_size >= look_back_size_) {
    access_history_.pop_back();
  }

  access_history_.push_front(nanos);
}

}  // namespace bustub
