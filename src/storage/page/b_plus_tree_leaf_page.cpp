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
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize();

  while (left < right) {
    int mid = left + (right - left) / 2;

    int comp_result = comparator(array[mid].first, key);

    if (comp_result < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // Bounds checking
  if (index < 0 || index >= GetSize()) {
    // This should not happen in correct usage, but return default key instead of crashing
    KeyType key{};
    return key;
  }
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // Bounds checking
  if (index < 0 || index >= GetSize()) {
    // This should not happen in correct usage, but return first element as fallback
    // Note: This assumes GetSize() > 0, which should be ensured by caller
    static MappingType dummy_item{};
    if (GetSize() > 0) {
      return array[0];
    }
    return dummy_item;
  }
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // Find the index where the key should be inserted
  int index = KeyIndex(key, comparator);

  // Check if key already exists (no duplicates allowed)
  if (index < GetSize() && comparator(array[index].first, key) == 0) {
    return GetSize();  // Key already exists, don't insert
  }

  // Check if page is full
  if (GetSize() >= GetMaxSize()) {
    return GetSize();  // Page is full, can't insert
  }

  // Shift elements to the right to make space
  for (int i = GetSize(); i > index; i--) {
    array[i] = array[i - 1];
  }

  // Insert the new key-value pair
  array[index] = std::make_pair(key, value);
  IncreaseSize(1);

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int total_size = GetSize();
  int move_size = total_size / 2;
  int start_index = total_size - move_size;

  // Copy the second half to the recipient
  for (int i = 0; i < move_size; i++) {
    recipient->array[i] = array[start_index + i];
  }

  // Update the size of both pages
  recipient->SetSize(move_size);
  SetSize(total_size - move_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int current_size = GetSize();

  // Copy the items to the end of the current array
  for (int i = 0; i < size; i++) {
    array[current_size + i] = items[i];
  }

  // Update the size
  SetSize(current_size + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  if (value == nullptr) {
    return false;
  }

  // Use binary search to find the key
  int index = KeyIndex(key, comparator);

  // Check if the key exists at the found index
  if (index < GetSize()) {
    int comp_result = comparator(array[index].first, key);

    if (comp_result == 0) {
      *value = array[index].second;
      return true;
    }
  }

  // Also try a linear search to see if we missed anything
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(array[i].first, key) == 0) {
      *value = array[i].second;
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int index = KeyIndex(key, comparator);

  // Check if the key exists
  if (index >= GetSize() || comparator(array[index].first, key) != 0) {
    return GetSize();  // Key doesn't exist
  }

  // Shift elements to the left to fill the gap
  for (int i = index; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }

  // Decrease the size
  IncreaseSize(-1);

  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // Copy all items from this page to the recipient
  recipient->CopyNFrom(array, GetSize());

  // Update the next page id of the recipient to point to this page's next page
  recipient->SetNextPageId(GetNextPageId());

  // Clear this page
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  if (GetSize() == 0) {
    return;  // Nothing to move
  }

  // Get the first item
  MappingType first_item = array[0];

  // Shift all elements to the left
  for (int i = 0; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }

  // Decrease the size
  IncreaseSize(-1);

  // Add the item to the end of the recipient
  recipient->CopyLastFrom(first_item);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int current_size = GetSize();

  // Check if there's space
  if (current_size >= GetMaxSize()) {
    return;  // No space to append
  }

  // Add the item to the end
  array[current_size] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  if (GetSize() == 0) {
    return;  // Nothing to move
  }

  // Get the last item
  int last_index = GetSize() - 1;
  MappingType last_item = array[last_index];

  // Decrease the size
  IncreaseSize(-1);

  // Add the item to the front of the recipient
  recipient->CopyFirstFrom(last_item);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int current_size = GetSize();

  // Check if there's space
  if (current_size >= GetMaxSize()) {
    return;  // No space to insert
  }

  // Shift all elements to the right
  for (int i = current_size; i > 0; i--) {
    array[i] = array[i - 1];
  }

  // Insert the item at the front
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
