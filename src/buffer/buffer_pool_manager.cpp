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
  if (page_id == INVALID_PAGE_ID) return nullptr;
  ASSERT(disk_manager_->IsPageFree(page_id)== false, "Fetching Free Pages");
  // this page is already inside the page table.
  if (page_table_.count(page_id)) {
    auto frame_of_page = page_table_[page_id];
    replacer_->Pin(frame_of_page);
    pages_[frame_of_page].pin_count_ += 1;
    return (pages_ + frame_of_page);
  }

  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    // do nothing
  } else
    return nullptr;
  ASSERT(frame_id != INVALID_FRAME_ID, "Invalid Frame Assignment");

  // write back
  if (page_on_frame.count(frame_id)) {
    auto old_page_id = page_on_frame[frame_id];
    if (pages_[frame_id].is_dirty_) disk_manager_->WritePage(old_page_id, pages_[frame_id].GetData());
    page_table_.erase(old_page_id);
    page_on_frame.erase(frame_id);
  }

  page_table_[page_id] = frame_id;
  page_on_frame[frame_id] = page_id;

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ += 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  return (pages_ + frame_id);
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {
    // do nothing
  } else
    return nullptr;

  ASSERT(frame_id != INVALID_FRAME_ID, "Invalid frame assignment");

  // write back
  if (page_on_frame.count(frame_id)) {
    auto old_page_id = page_on_frame[frame_id];
    if (pages_[frame_id].is_dirty_) disk_manager_->WritePage(old_page_id, pages_[frame_id].GetData());
    page_table_.erase(old_page_id);
    page_on_frame.erase(frame_id);
  }

  page_id_t new_page_id = AllocatePage();
  ASSERT(new_page_id != INVALID_PAGE_ID, "Invalid Page Allocation");

  // insert into maps
  page_table_.insert(std::make_pair(new_page_id, frame_id));
  page_on_frame.insert(std::make_pair(frame_id, new_page_id));

  // fresh this frame
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;

  page_id = new_page_id;
  return (pages_ + frame_id);
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {

  if (!page_table_.count(page_id)) {
    DeallocatePage(page_id);
    return true;
  }
  frame_id_t frame_of_page = page_table_[page_id];
  if (pages_[frame_of_page].GetPinCount() != 0) return false;

  page_table_.erase(page_id);
  page_on_frame.erase(frame_of_page);
  free_list_.push_back(frame_of_page);

  DeallocatePage(page_id);
  pages_[frame_of_page].ResetMemory();
  pages_[frame_of_page].is_dirty_ = false;
  pages_[frame_of_page].page_id_ = INVALID_PAGE_ID;
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table_.count(page_id) == 0) {
    LOG(INFO) << "no such page id " << page_id;
    return false;
  }
  auto frame_id = page_table_[page_id];
  Page *p = &pages_[frame_id];
  if (is_dirty) p->is_dirty_ = true;
  ASSERT(p->pin_count_ >= 0, "PAGE PIN COUNT INVALID");
  --p->pin_count_;
  if (p->pin_count_) return false;
  replacer_->Unpin(frame_id);
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.count(page_id)) {
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
    return true;
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
