//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <vector>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    // Initialize the list of unpinned frames
    unpinned_frames = std::vector<frame_id_t>();
    // Set the capacity of the LRUReplacer
    unpinned_frames.reserve(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(lru_mutex);
  
  // Check if there's any frame to evict
  if (unpinned_frames.empty()) {
    return false;
  }

    // Get the least recently used frame (front of the list)
    *frame_id = unpinned_frames.front();
    // Remove the least recently used frame from the list
    unpinned_frames.erase(unpinned_frames.begin());
    // Return true indicating a frame was evicted
    return true;
    // If no frame was evicted, return false
  
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(lru_mutex);
    
    // Iterate through the list of unpinned frames to find the frame to pin
    for (auto it = unpinned_frames.begin(); it != unpinned_frames.end(); ++it) {
        if (*it == frame_id) {
            // Remove the frame from the list
            unpinned_frames.erase(it);
            break;
        }
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(lru_mutex);

    if (frame_id < 0) {
        // Invalid frame ID, do nothing
        return;
    }
    // Check if the frame is already in the list of unpinned frames
    // If it's already in the map, it's already unpinned
    for (auto it = unpinned_frames.begin(); it != unpinned_frames.end(); ++it) {
        if (*it == frame_id) {
            // Frame is already unpinned, no need to do anything
            return;
        }
    }
    // Frame is not in the list, unpin it by adding it to the list of unpinned frames
    // Add the frame to the back of the list (most recently used position)
    unpinned_frames.push_back(frame_id);
}

size_t LRUReplacer::Size() { return unpinned_frames.size(); }

}  // namespace bustub
