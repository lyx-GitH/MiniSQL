#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include <unordered_map>
#include "common/macros.h"
using raw_ptr = int64_t;
#define RAW(ptr) (reinterpret_cast<int64_t>(ptr))
#define TO_PTR(raw) (reinterpret_cast<void *>(raw))

struct PtrHeap {
  std::unordered_set<raw_ptr> Used;
  std::unordered_set<raw_ptr> NotUsed;

  PtrHeap() : Used(), NotUsed(){};
};

class MemHeap {
 public:
  virtual ~MemHeap() = default;

  /**
   * @brief Allocate a contiguous block of memory of the given size
   * @param size The size (in bytes) of memory to allocate
   * @return A non-null pointer if allocation is successful. A null pointer if
   * allocation fails.
   */
  virtual void *Allocate(size_t size) = 0;

  /**
   * @brief Returns the provided chunk of memory back into the pool
   */
  virtual void Free(void *ptr) = 0;
};

// class SimpleMemHeap : public MemHeap {
// public:
//   ~SimpleMemHeap() {
//     for (auto it: allocated_) {
//       free(it);
//     }
//   }
//
//   void *Allocate(size_t size) {
//     void *buf = malloc(size);
//     ASSERT(buf != nullptr, "Out of memory exception");
//     allocated_.insert(buf);
//     return buf;
//   }
//
//   void Free(void *ptr) {
//     if (ptr == nullptr) {
//       return;
//     }
//     auto iter = allocated_.find(ptr);
//     if (iter != allocated_.end()) {
//       allocated_.erase(iter);
//     }
//   }
//
// private:
//   std::unordered_set<void *> allocated_;
// };

class SimpleMemHeap : public MemHeap {
 public:
  ~SimpleMemHeap() override {
    for (auto it : allocated_) free(it.first);
  }

  void *Allocate(size_t size) override {
    auto it = fake_ptr_.find(size);
    if (it != fake_ptr_.end() && !it->second.NotUsed.empty()) {
      // There exists a pointer that is free to use
      auto &Used = it->second.Used;
      auto &NotUsed = it->second.NotUsed;
      auto raw_p = *NotUsed.begin();
      NotUsed.erase(NotUsed.begin());
      Used.insert(raw_p);
      return TO_PTR(raw_p);
    } else {
      // No pointer is free to use
      void *mem = malloc(size);
      allocated_.insert(std::make_pair(mem, size));
      if (it == fake_ptr_.end()) {
        fake_ptr_.insert(std::make_pair(size, PtrHeap()));
        fake_ptr_[size].Used.insert(RAW(mem));
      } else {
        assert(it->second.NotUsed.empty());
        it->second.Used.insert(RAW(mem));
      }

      return mem;
    }
  }

  void Free(void *ptr) override {
    if (ptr == nullptr) return;
    auto it = allocated_.find(ptr);
    if (it != allocated_.end()) {
      fake_ptr_[it->second].Used.erase(RAW(ptr));
      fake_ptr_[it->second].NotUsed.insert(RAW(ptr));
    } else {
      ASSERT(false, "MemHeap::Free : Pointer Not Assigned By This Heap");
    }
  }

 private:
  //  std::list<void*> allocated_;
  std::unordered_map<size_t, PtrHeap> fake_ptr_;
  std::unordered_map<void *, size_t> allocated_;
};

#endif  // MINISQL_MEM_HEAP_H
