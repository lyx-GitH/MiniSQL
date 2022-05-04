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

page_id_t DiskManager::GetBlockId(page_id_t logical_page_id) { return logical_page_id / BITMAP_SIZE; }

page_id_t DiskManager::GetLocalId(page_id_t logical_page_id) { return logical_page_id % BITMAP_SIZE; }

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  auto n_block = GetLocalId(logical_page_id);
  auto n_local = GetBlockId(logical_page_id);
  return n_block * (1 + BITMAP_SIZE) + 1 + n_local + 1;
}

page_id_t DiskManager::GetMetaIdP(page_id_t logical_page_id) {
  auto n_block = GetBlockId(logical_page_id);
  auto logical_leading_id = n_block * BITMAP_SIZE;
  return MapPageId(logical_leading_id) - 1;
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
  auto *dMeta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  if (dMeta->num_allocated_pages_ > MAX_VALID_PAGE_ID) return INVALID_PAGE_ID;

  uint32_t i;

  char buf[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);

  for (i = 0; i < dMeta->num_extents_ && dMeta->extent_used_page_[i] >= BITMAP_SIZE; i++)
    ;

  if (i >= dMeta->num_extents_) {
    auto *bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
    bit_map->AllocatePage(i);

    ASSERT(i == 0, "DiskManager::Allocate->NewPageAllocate ERROR");


    auto meta_p_id = GetMetaIdP(dMeta->num_allocated_pages_);

    dMeta->extent_used_page_[dMeta->num_extents_] = 1;
    dMeta->num_allocated_pages_ += 1;
    dMeta->num_extents_ += 1;
    WritePhysicalPage(meta_p_id, reinterpret_cast<const char *>(bit_map));
    WritePhysicalPage(META_PAGE_ID, reinterpret_cast<const char *>(dMeta));

    return dMeta->num_allocated_pages_ - 1;
  } else {
    auto l_leading_id = i * BITMAP_SIZE;
    auto p_meta_id = MapPageId(l_leading_id) - 1;

    ReadPhysicalPage(p_meta_id, buf);
    auto *bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);

    uint32_t local_l_id = -1;
    bit_map->AllocatePage(local_l_id);

    ASSERT(local_l_id != (uint32_t)-1, "DiskManager::Allocate->InsertAllocate ERROR");
    dMeta->num_allocated_pages_ += 1;
    dMeta->extent_used_page_[i] += 1;

    WritePhysicalPage(p_meta_id, buf);
    WritePhysicalPage(META_PAGE_ID, reinterpret_cast<const char *>(dMeta));

    return i * BITMAP_SIZE + local_l_id;
  }
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto local_id = GetLocalId(logical_page_id);
  auto block_id = GetBlockId(logical_page_id);
  auto meta_p_id = GetMetaIdP(logical_page_id);
  char buf[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);
  ReadPhysicalPage(meta_p_id, buf);

  auto *bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  auto *dMeta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  ASSERT(bit_map->DeAllocatePage(local_id), "Free NULL page id");

  dMeta->num_allocated_pages_ -= 1;
  dMeta->extent_used_page_[block_id] -= 1;

  WritePhysicalPage(meta_p_id, buf);
  WritePhysicalPage(META_PAGE_ID, reinterpret_cast<const char *>(dMeta));
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  auto local_id = GetLocalId(logical_page_id);
  auto meta_p_id = GetMetaIdP(logical_page_id);
  char buf[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);
  ReadPhysicalPage(meta_p_id, buf);

  auto *bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);

  return bit_map->IsPageFree(local_id);
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
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
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
