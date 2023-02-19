//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  //  std::cout << "BPMI: " << pool_size << " replacer " << replacer_k << std::endl;
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  //  LOG_DEBUG("NewPage:");
  //  std::cout << "NP" << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t free_frame_id;
  page_id_t new_page_id;
  Page *free_page = nullptr;

  //  latch_.lock();
  if (!free_list_.empty()) {
    new_page_id = AllocatePage();
    *page_id = new_page_id;

    free_frame_id = free_list_.front();
    free_page = &pages_[free_frame_id];
    free_list_.pop_front();
    //    latch_.unlock();

    // Should take WLock on page first
    free_page->WLatch();
    ResetPageMetadata(*free_page);
    free_page->ResetMemory();
    free_page->page_id_ = new_page_id;
    free_page->pin_count_ = 1;
    free_page->WUnlatch();

    PostSuccessfullPageAllocation(new_page_id, free_frame_id);
    return free_page;
  }
  //  latch_.unlock();

  if (replacer_->Evict(&free_frame_id)) {
    free_page = &pages_[free_frame_id];
    page_table_->Remove(free_page->GetPageId());

    new_page_id = AllocatePage();
    *page_id = new_page_id;

    free_page->WLatch();
    if (free_page->IsDirty()) {
      FlushPgImpInternal(*free_page);
    }
    ResetPageMetadata(*free_page);
    free_page->ResetMemory();
    free_page->page_id_ = new_page_id;
    free_page->pin_count_ = 1;
    free_page->WUnlatch();

    PostSuccessfullPageAllocation(new_page_id, free_frame_id);
  }

  return free_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  //  LOG_DEBUG("FetchPage: %d", page_id);
  //  std::cout << "FP " << page_id << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *existing_page = &pages_[frame_id];
    existing_page->WLatch();
    existing_page->pin_count_ = existing_page->GetPinCount() + 1;
    PostSuccessfullPageAllocation(page_id, frame_id);
    existing_page->WUnlatch();
    return existing_page;
  }

  // Page is not already present in buffer pool, that means, we need to fetch it from the disk and for that
  // We need to see whether there is an empty frame in freelist or not
  //  latch_.lock();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    //    latch_.unlock();

    Page *existing_page = &pages_[frame_id];
    existing_page->WLatch();
    existing_page->ResetMemory();
    disk_manager_->ReadPage(page_id, existing_page->GetData());
    existing_page->page_id_ = page_id;
    //    existing_page->pin_count_ = existing_page->GetPinCount() + 1;
    existing_page->pin_count_ = 1;
    existing_page->WUnlatch();

    PostSuccessfullPageAllocation(page_id, frame_id);
    return existing_page;
  }
  //  latch_.unlock();

  if (replacer_->Evict(&frame_id)) {
    Page *existing_page = &pages_[frame_id];
    page_table_->Remove(existing_page->GetPageId());

    existing_page->WLatch();
    if (existing_page->IsDirty()) {
      FlushPgImpInternal(*existing_page);
    }
    ResetPageMetadata(*existing_page);
    existing_page->ResetMemory();
    disk_manager_->ReadPage(page_id, existing_page->GetData());
    existing_page->page_id_ = page_id;
    //    existing_page->pin_count_ = existing_page->GetPinCount() + 1;
    existing_page->pin_count_ = 1;
    existing_page->WUnlatch();

    PostSuccessfullPageAllocation(page_id, frame_id);
    return existing_page;
  }

  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  //  LOG_DEBUG("Unpin Page: %d, %d", page_id, is_dirty);
  //  std::cout << "UP " << page_id << " " << is_dirty << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *existing_page = &pages_[frame_id];
  existing_page->WLatch();
  if (existing_page->GetPinCount() <= 0) {
    existing_page->WUnlatch();
    return false;
  }

  if (is_dirty) {
    existing_page->is_dirty_ = is_dirty;
  }

  if (existing_page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  } else {
    existing_page->pin_count_ = existing_page->GetPinCount() - 1;
    if (existing_page->GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
  }
  existing_page->WUnlatch();

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  //  LOG_DEBUG("FlushPage: %d", page_id);
  //  std::cout << "FLP " << page_id << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *existing_page = &pages_[frame_id];
  existing_page->WLatch();
  FlushPgImpInternal(*existing_page);
  existing_page->WUnlatch();

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  //  LOG_DEBUG("FlushAllPages:");
  //  std::cout << "FLAP" << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    Page *page = &pages_[frame_id];
    page->WLatch();
    FlushPgImpInternal(*page);
    page->WUnlatch();
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  //  LOG_DEBUG("DeletePage: %d", page_id);
  //  std::cout << "DP " << page_id << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *existing_page = &pages_[frame_id];
  existing_page->WLatch();

  if (existing_page->GetPinCount() > 0) {
    existing_page->WUnlatch();
    return false;
  }

  if (existing_page->IsDirty()) {
    FlushPgImpInternal(*existing_page);
  }

  ResetPageMetadata(*existing_page);
  existing_page->ResetMemory();
  existing_page->WUnlatch();

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  //  {
  //    std::lock_guard<std::mutex> lock(latch_);
  //    free_list_.emplace_back(frame_id);
  //  }
  free_list_.emplace_back(frame_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::FlushPgImpInternal(Page &page) {
  disk_manager_->WritePage(page.GetPageId(), page.GetData());
  page.is_dirty_ = false;
}

void BufferPoolManagerInstance::PostSuccessfullPageAllocation(page_id_t page_id, frame_id_t frame_id) {
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
}

void BufferPoolManagerInstance::ResetPageMetadata(Page &page) {
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;
  //  page.pin_count_ = 0;
}

}  // namespace bustub
