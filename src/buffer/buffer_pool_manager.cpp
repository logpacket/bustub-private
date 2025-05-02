//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/config.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();

  std::unordered_map<page_id_t, frame_id_t>::iterator search = page_table_.find(page_id);
  Page *retPage = nullptr;
  // 1.1 If P exists, pin it and return it immediately
  if (search != page_table_.end()) {
    frame_id_t f_id = search->second;
    retPage = &pages_[f_id];

    // Pin the page by incrementing pin count
    retPage->pin_count_++;
    // Remove from replacer since it's now pinned
    replacer_->Pin(f_id);

    latch_.unlock();
    return retPage;
  }

  // The page requested is not in the memory yet.
  // Find the frame to contatin the requested page
  frame_id_t page_frame_id;
  Page *replPage;
  if (!free_list_.empty()) {
    page_frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_.insert({page_id, page_frame_id});
    replPage = &pages_[page_frame_id];
    replPage->page_id_ = page_id;
    replPage->pin_count_ = 1;
    replPage->is_dirty_ = false;
    replacer_->Pin(page_frame_id);
    disk_manager_->ReadPage(page_id, replPage->GetData());
    latch_.unlock();
    return replPage;
  }
  // If free list is empty, find a victim page from the replacer
  if (!replacer_->Victim(&page_frame_id)) {
    // No victim found, all pages are pinned
    latch_.unlock();
    return nullptr;
  }
  // Get the replacement page
  replPage = &pages_[page_frame_id];

  // 2. If R is dirty, write it back to the disk
  if (replPage->IsDirty()) {
    disk_manager_->WritePage(replPage->GetPageId(), replPage->GetData());
  }

  // Reset the page's memory and metadata
  page_table_.erase(page_table_.find(replPage->GetPageId()));
  page_table_.insert({page_id, page_frame_id});

  replPage->page_id_ = page_id;
  replPage->pin_count_ = 1;
  replPage->is_dirty_ = false;
  replacer_->Pin(page_frame_id);
  replPage->ResetMemory();
  // Read the requested page from disk
  disk_manager_->ReadPage(page_id, replPage->GetData());

  latch_.unlock();
  return replPage;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  frame_id_t unpinned_frame_id = page_table_.find(page_id)->second;
  Page *unpinned_page = &pages_[unpinned_frame_id];
  if (unpinned_page->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  frame_id_t victim_frame = page_table_.find(page_id)->second;
  replacer_->Unpin(victim_frame);

  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }

  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  latch_.unlock();
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();

  // Check if we can allocate a new page
  if (free_list_.empty() && replacer_->Size() == 0) {
    // All pages are pinned, cannot allocate a new page
    latch_.unlock();
    return nullptr;
  }

  // Allocate a new page ID from the disk manager
  page_id_t new_page_id = disk_manager_->AllocatePage();

  // Set the output parameter
  *page_id = new_page_id;

  frame_id_t frame_id;
  Page *page = nullptr;

  // Try to get a page from the free list first
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // Otherwise, find a victim page from the replacer
    if (!replacer_->Victim(&frame_id)) {
      // This shouldn't happen since we checked above, but just to be safe
      latch_.unlock();
      return nullptr;
    }

    // Get the page being replaced
    page = &pages_[frame_id];

    // If the page is dirty, write it back to disk
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }

    // Remove the victim page from the page table
    page_table_.erase(page->GetPageId());
  }

  // Get the page we will use
  page = &pages_[frame_id];

  // Reset the page's memory and update metadata
  page->ResetMemory();
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  // Add the page to the page table and pin it in the replacer
  page_table_[new_page_id] = frame_id;
  replacer_->Pin(frame_id);

  latch_.unlock();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
