#include <sys/stat.h>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

/*
 * A single extent block contains BITMAP_SIZE data pages
 * p_block_id -> physical block index of that logical page
 * p_local_id -> physical local index (index in that block) of that logical page
 * p_lead_id -> physical id of the bit map page of that logical page
 */

static constexpr std::size_t max_extent_size = DiskManager::BITMAP_SIZE;

page_id_t DiskManager::GetBlockId(page_id_t logical_page_id) { return logical_page_id / max_extent_size; }

page_id_t DiskManager::GetLocalId(page_id_t logical_page_id) { return logical_page_id % max_extent_size; }

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  auto extend_id = logical_page_id / max_extent_size;
  auto local_id = logical_page_id % max_extent_size;
  page_id_t physical_id = 1 + extend_id * (1 + max_extent_size) + local_id + 1;
  return physical_id;
}

page_id_t DiskManager::GetBitMapPhysicalId(page_id_t logical_page_id) {
  auto physic_id = MapPageId(logical_page_id);
  auto block_id = logical_page_id % max_extent_size;
  return physic_id - block_id - 1;
}

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  auto disk_meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  std::size_t i = 0;
  for (i = 0; i < disk_meta_page->GetExtentNums(); i++)
    if (disk_meta_page->GetExtentUsedPage(i) < max_extent_size) break;
  if (i >= disk_meta_page->GetExtentNums()) {
    disk_meta_page->extent_used_page_[i] = 0;
    disk_meta_page->num_extents_ += 1;
  }
  disk_meta_page->num_allocated_pages_ += 1;
  disk_meta_page->extent_used_page_[i] += 1;
  page_id_t bm_logical_id = i * (1 + max_extent_size) + 1;
  char buf[PAGE_SIZE] = {0};
  ReadPhysicalPage(bm_logical_id, buf);
  auto bit_map_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  uint32_t new_local_id;

  bit_map_page->AllocatePage(new_local_id);
  page_id_t new_logical_id = max_extent_size * i + new_local_id;
  ASSERT(bm_logical_id == GetBitMapPhysicalId(new_logical_id), "Error");
  WritePhysicalPage(0, meta_data_);
  WritePhysicalPage(bm_logical_id, buf);
  return new_logical_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if(IsPageFree(logical_page_id))
    return;
  //ASSERT(IsPageFree(logical_page_id) == false, "Free empty page");
  auto bit_map_id = GetBitMapPhysicalId(logical_page_id);
  auto local_id = logical_page_id % max_extent_size;
  auto extent_id = logical_page_id / max_extent_size;
  char buf[PAGE_SIZE] = {0};
  ReadPhysicalPage(bit_map_id, buf);
  reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf)->DeAllocatePage(local_id);
  auto disk_meta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  //ASSERT(disk_meta->extent_used_page_[extent_id] > 0, "Dealloctae null extent");
//  ASSERT(r, "Does not deallocate a page");

  disk_meta->extent_used_page_[extent_id] -= 1;
  disk_meta->num_allocated_pages_ -= 1;
  WritePhysicalPage(bit_map_id, buf);
  WritePhysicalPage(0, meta_data_);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  auto bit_map_id = GetBitMapPhysicalId(logical_page_id);
  auto local_id = logical_page_id % max_extent_size;
  char buf[PAGE_SIZE] = {0};
  ReadPhysicalPage(bit_map_id, buf);
  auto bit_map_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  return bit_map_page->IsPageFree(local_id);
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);

      if(physical_page_id == 2) {
        uint32_t mn;
        memcpy(&mn, page_data, 4);
        LOG(INFO) <<"when reading meta page, magic num = "<<mn<<std::endl;
      }
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  if(physical_page_id == 2) {
    uint32_t mn;
    memcpy(&mn, page_data, 4);
    std::cout <<"when writing meta page, magic num = "<<mn<<std::endl;
  }
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
