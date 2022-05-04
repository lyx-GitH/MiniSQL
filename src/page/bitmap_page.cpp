#include "page/bitmap_page.h"
#include <glog/logging.h>

#define CHAR_WIDTH 3
#define CHAR_SIZE 8
#define CHAR_TAIL 7

//equals to: p/8, p%8
#define split(p) p >> CHAR_WIDTH, p &CHAR_TAIL


template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= GetMaxSupportedSize()) return false;
  page_offset = next_free_page_;
  setTrue(split(page_offset));
  page_allocated_ += 1;
  next_free_page_ = find_next_free_page();
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {

  if (page_offset >= GetMaxSupportedSize() || IsPageFreeLow(split(page_offset)))
    return false;
  else {
    setFalse(split(page_offset));
    next_free_page_ = page_offset;
    page_allocated_ -= 1;
    return true;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(split(page_offset));
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return !(((bytes[byte_index]) >> (uint32_t)(bit_index)) & 1);
}

template <size_t PageSize>
void BitmapPage<PageSize>::setTrue(uint32_t byte_index, uint8_t bit_index) {
  // uint32_t mask = 1 << bit_index;
  bytes[byte_index] |= (1 << bit_index);
}

template <size_t PageSize>
void BitmapPage<PageSize>::setFalse(uint32_t byte_index, uint8_t bit_index) {
  //  uint32_t mask = ~(1 << bit_index);
  bytes[byte_index] &= (~(1 << bit_index));
}

template <size_t PageSize>
uint32_t BitmapPage<PageSize>::find_next_free_page() {
  if (IsPageFree(page_allocated_)) return page_allocated_;

  for (uint32_t i = 0; i < MAX_CHARS; i++) {
    if (bytes[i] != 255) {
      //not full
      for (uint8_t j = 0; j < CHAR_SIZE; j++) {
        //equals to: i*8 + j
        if (IsPageFreeLow(i, j)) return (i << 3) | j;
      }
    }
  }

  return INVALID_PAGE_ID;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;