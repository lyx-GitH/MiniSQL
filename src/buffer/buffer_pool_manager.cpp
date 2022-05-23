#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  if(page_id == INVALID_PAGE_ID)
    return nullptr;
  auto map_it = page_table_.find(page_id);

  if (map_it != page_table_.end()) {
    replacer_->Pin(map_it->second);
    pages_[map_it->second].pin_count_++;

    return &pages_[map_it->second];
  } else  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  {
    frame_id_t frame_id;      // frame id of R
    page_id_t p_id;           // page id
    if (!free_list_.empty())  // Note that pages are always found from the free list first.
    {
      frame_id = free_list_.front();
      free_list_.pop_front();
    }

    else  // find in replacer
    {
      if (!(*replacer_).Victim(&frame_id))  // doesn't find neither
      {
        return nullptr;
      }
    }

    auto map_it0 = page_table_.find(frame_id);  // find R from the page table

    if (map_it0 != page_table_.end()) {
      p_id = map_it0->first;
      if (pages_[frame_id].is_dirty_) {
        disk_manager_->WritePage(p_id, pages_[frame_id].GetData());
      }
      page_table_.erase(map_it0);
    }

    page_table_[p_id] = frame_id;
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_++;

    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    return &pages_[frame_id];
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  frame_id_t frame_id;  // frame id of P

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (!free_list_.empty())  // pick from the free list first
  {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  else  // pick from the replacer
  {
    if (!(*replacer_).Victim(&frame_id)) {
      return nullptr;
    }
  }

  page_id_t next_page_id = AllocatePage();

  auto map_it = page_table_.begin();

  for (; map_it != page_table_.end(); ++map_it)  // if the frame_id is in the map, erase it
  {
    if (map_it->second == frame_id) {
      page_table_.erase(map_it);
      break;
    }
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = next_page_id;
  pages_[frame_id].pin_count_ = 1;
  page_table_.insert(std::make_pair(next_page_id, frame_id));

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = next_page_id;
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  auto map_it1 = page_table_.find(page_id);

  // 1.   If P does not exist, return true.
  if (map_it1 == page_table_.end())
    return true;
  else {
    Page *p;
    for (p = pages_; p->page_id_ != INVALID_PAGE_ID; ++p) {
      if (p->page_id_ == page_id)  // find the page to be flushed
        break;
    }

    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if (p->GetPinCount() != 0) return false;
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata
    //      and return it to the free list.
    else {
      free_list_.push_back(map_it1->second);
      page_table_.erase(page_id);
      DeallocatePage(page_id);

      p->page_id_ = INVALID_PAGE_ID;
      p->is_dirty_ = false;
      p->ResetMemory();

      return true;
    }
  }
  //  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  size_t cnt = 0;
  for (Page *p = pages_; cnt < pool_size_; ++p, ++cnt) {
    if (p->page_id_ == page_id)  // find the page to be unpinned
    {
      if (is_dirty) p->is_dirty_ = true;

      --p->pin_count_;

      if (p->pin_count_ == 0) {
        auto map_it = page_table_.find(p->page_id_);
        if (map_it != page_table_.end()) {
          replacer_->Unpin(map_it->second);
          return true;
        }
      }
      break;
    }
  }

  return false;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  Page *p = pages_;

  for (; p->page_id_ != INVALID_PAGE_ID; ++p) {
    if (p->page_id_ == page_id)  // find the page to be flushed
    {
      if (p->pin_count_ == 0) {
        disk_manager_->WritePage(page_id, p->GetData());
        return true;
      }
      break;
    }
  }

  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug

bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
