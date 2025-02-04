//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  class FrameAccessHistory {
   public:
    explicit FrameAccessHistory(frame_id_t frame_id, size_t look_back_size, bool is_evictable = false)
        : is_evictable_(is_evictable), look_back_size_(look_back_size), frame_id_(frame_id) {}
    void RecordAccess(int record_access_count_);
    inline auto GetAccessHistorySize() -> size_t { return access_history_.size(); }
    inline auto GetLookBackSize() -> size_t { return look_back_size_; }
    inline auto GetKthAccessRecord() -> int64_t { return access_history_.back(); }
    inline auto GetLRUAccessRecord() -> int64_t { return access_history_.front(); }
    inline auto GetLastAccessRecord() -> int64_t { return access_history_.back(); }
    inline auto GetFrameId() -> frame_id_t { return frame_id_; }
    inline auto SetEvictable(bool evictable) -> void { is_evictable_ = evictable; }
    inline auto IsEvictable() -> bool { return is_evictable_; }

   private:
    bool is_evictable_;
    size_t look_back_size_;
    frame_id_t frame_id_;
    std::list<int64_t> access_history_;
    std::mutex frame_latch_;
  };

  class FrameAccessHistoryOrderComparison {
   public:
    auto operator()(const std::shared_ptr<FrameAccessHistory> &frame1,
                    const std::shared_ptr<FrameAccessHistory> &frame2) const {
      size_t frame1_size = frame1->GetAccessHistorySize();
      size_t frame2_size = frame2->GetAccessHistorySize();

      bool frame1_hist_available = frame1_size == frame1->GetLookBackSize();
      bool frame2_hist_available = frame2_size == frame2->GetLookBackSize();
      bool return_val = true;

      if (frame1_hist_available && !frame2_hist_available) {
        // retain frame1
        return_val = false;
      } else if (frame2_hist_available && !frame1_hist_available) {
        // retain frame2
        return_val = true;
      } else if (!frame2_hist_available && !frame1_hist_available) {
        // LRU case
        return_val = frame1->GetLastAccessRecord() <= frame2->GetLastAccessRecord();
      } else {
        // Both frame's history is available
        return_val = frame1->GetKthAccessRecord() <= frame2->GetKthAccessRecord();
      }

      return !return_val;
    }
  };

  auto RemoveFrameFromSetInternal(frame_id_t frame_id) -> void;
  //  };
  size_t k_;
  std::mutex latch_;
  std::unordered_map<frame_id_t, std::shared_ptr<FrameAccessHistory>> frame_index_map_;
  std::set<std::shared_ptr<FrameAccessHistory>, FrameAccessHistoryOrderComparison> frame_history_set_;
  std::atomic<int> record_causal_relation_addr_;
  size_t replacer_size_;
};

}  // namespace bustub
