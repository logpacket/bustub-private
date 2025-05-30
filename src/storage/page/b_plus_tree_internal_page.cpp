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

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++)
    if (array[i].second == value) return i;
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  auto k_it = std::lower_bound(
      array + 1, array + GetSize(), key,
      [&comparator](const MappingType &pair, const KeyType &key) { return comparator(pair.first, key) < 0; });

  if (k_it == array + GetSize()) {
    return ValueAt(GetSize() - 1);  // Return the last value if key is greater than all keys
  }

  if (comparator(k_it->first, key) == 0) {
    return k_it->second;  // Return the value if key matches
  }
  return std::prev(k_it)->second;  // Return the previous value if key does not match
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // A new root has size 2: one for old_value, one for new_value
  array[0].second = old_value;  // First pair's key is not used, only value (old_value) matters
  array[1].first = new_key;     // Set the key for the second pair
  array[1].second = new_value;  // Set the value for the second pair
  SetSize(2);                   // Update the size of the page to 2 entries
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  auto new_value_idx = ValueIndex(old_value) + 1;
  std::move_backward(array + new_value_idx, array + GetSize(), array + GetSize() + 1);
  array[new_value_idx].first = new_key;
  array[new_value_idx].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  auto start_idx = GetSize() / 2;
  int size = GetSize() - start_idx;

  // Copy entries from start_idx to the end to the recipient page
  recipient->CopyNFrom(array + start_idx, size, buffer_pool_manager);

  // Update the size of the current page
  SetSize(start_idx);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array + GetSize());

  for (int i = 0; i < size; i++) {
    auto page = buffer_pool_manager->FetchPage(items[i].second);
    BPlusTreePage *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  std::move(array + index + 1, array + GetSize(), array + index);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // For an internal page, there should be exactly 1 element (only child)
  // with SIZE = 1 to be removed
  if (GetSize() != 1) {
    throw Exception(ExceptionType::INVALID, "RemoveAndReturnOnlyChild called on a page with size != 1");
  }

  // Get the only child's page_id before removing it
  ValueType only_child = array[0].second;

  // Clear the page (reset size to 0)
  SetSize(0);

  return only_child;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // Get the current size of the recipient page to determine where to start inserting
  int start_idx = recipient->GetSize();

  // The middle key should be prepended to the first key of this page
  // This is because when merging, the parent key that separates the two pages
  // needs to be moved down to maintain the B+ tree property
  if (GetSize() > 0) {
    // Set the middle key as the first key for the first item that's being moved
    // (Because the first item's key is not used in B+ tree internal pages)
    recipient->array[start_idx].first = middle_key;
    recipient->array[start_idx].second = array[0].second;

    // Update the parent page id of the child page
    page_id_t child_page_id = array[0].second;
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    if (child_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Unable to fetch child page from buffer pool manager");
    }

    // Update parent page id and mark as dirty
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);

    // Move the rest of the items (from index 1 onwards)
    for (int i = 1; i < GetSize(); i++) {
      recipient->array[start_idx + i].first = array[i].first;
      recipient->array[start_idx + i].second = array[i].second;

      // Update parent page id for each child
      child_page_id = array[i].second;
      child_page = buffer_pool_manager->FetchPage(child_page_id);
      if (child_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "Unable to fetch child page from buffer pool manager");
      }

      child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(recipient->GetPageId());
      buffer_pool_manager->UnpinPage(child_page_id, true);
    }

    // Update the recipient's size
    recipient->SetSize(start_idx + GetSize());

    // Clear this page
    SetSize(0);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // Get the first mapping in this page
  MappingType pair = array[0];

  // Create a new mapping with the middle key and the value from our first entry
  MappingType new_pair{middle_key, pair.second};

  // Remove the first element from this page
  std::move(array + 1, array + GetSize(), array + 0);
  IncreaseSize(-1);

  // Add the new mapping to the end of the recipient page
  recipient->CopyLastFrom(new_pair, buffer_pool_manager);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  *(array + GetSize()) = pair;
  IncreaseSize(1);

  auto page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // Get the last mapping in this page
  MappingType pair = array[GetSize() - 1];

  // Remove the last element from this page
  IncreaseSize(-1);

  // Create a new mapping with the middle key and the value from our last entry
  // For internal pages, when we move entries between nodes, we need to use the parent's key (middle_key)
  MappingType new_pair{middle_key, pair.second};

  // Add the new mapping to the beginning of the recipient page
  recipient->CopyFirstFrom(new_pair, buffer_pool_manager);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  std::move_backward(array, array + GetSize(), array + GetSize() + 1);
  *(array) = pair;
  IncreaseSize(1);

  auto page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
