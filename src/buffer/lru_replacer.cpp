#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { this->num_pages = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list.size() == 0) return false;
  *frame_id = lru_list.back();
  lru_list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  list<frame_id_t>::iterator it = lru_list.begin();
  for (; it != lru_list.end(); ++it)  // to find the location of the frame_id
  {
    if (*it == frame_id) break;  // find
  }

  if (it != lru_list.end()) lru_list.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  list<frame_id_t>::iterator it = lru_list.begin();
  for (; it != lru_list.end(); ++it)  // to find whether the frame_id exists
  {
    if (*it == frame_id) break;
  }
  if (it == lru_list.end())  // doesn't exist
  {
    if (lru_list.size() == num_pages)  // full
    {
      lru_list.pop_back();            // remove the least recently used page
      lru_list.push_front(frame_id);  // add the new page to the top of the list
    } else                            // not full
    {
      lru_list.push_front(frame_id);
    }
  }

  //  else // exists
  //  {
  //
  //    // overlook it
  //
  //  }
}

size_t LRUReplacer::Size() { return lru_list.size(); }
