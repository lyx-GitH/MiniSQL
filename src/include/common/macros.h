#ifndef MINISQL_MACROS_H
#define MINISQL_MACROS_H

#include <cassert>

#define ASSERT(expr, message) assert((expr) && (message))

#define SAY_HELLO do{system("figlet -c -f slant MiniSQL ready| lolcat"); } while(0);

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                             \
  cname(const cname &) = delete;            /* NOLINT */ \
  cname &operator=(const cname &) = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                        \
  cname(cname &&) = delete;            /* NOLINT */ \
  cname &operator=(cname &&) = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

#define MACH_WRITE_TO(Type, Buf, Data) \
           do { \
              *reinterpret_cast<Type *>(Buf) = (Data); \
           } while (0)
#define MACH_WRITE_UINT32(Buf, Data) MACH_WRITE_TO(uint32_t, (Buf), (Data))
#define MACH_WRITE_INT32(Buf, Data) MACH_WRITE_TO(int32_t, (Buf), (Data))
#define MACH_WRITE_UINT64(Buf, Data) MACH_WRITE_TO(uint64_t, (Buf), (Data))
#define MACH_WRITE_STRING(Buf, Str)      \
           do {                                       \
              memcpy(Buf, Str.c_str(), Str.length()); \
           } while (0)

#define MACH_READ_FROM(Type, Buf) (*reinterpret_cast<const Type *>(Buf))
#define MACH_READ_UINT32(Buf) MACH_READ_FROM(uint32_t, (Buf))
#define MACH_READ_INT32(Buf) MACH_READ_FROM(int32_t, (Buf))
#define MACH_READ_UINT64(Buf) MACH_READ_FROM(uint64_t, (Buf))

#define MACH_STR_SERIALIZED_SIZE(Str) (4 + Str.length())

#define ALLOC(Heap, Type) new(Heap.Allocate(sizeof(Type)))Type
#define ALLOC_P(Heap, Type) new(Heap->Allocate(sizeof(Type)))Type
#define ALLOC_COLUMN(Heap) ALLOC(Heap, Column)
#define ALLOC_P_COLUMN(Heap) ALLOC_P(Heap, Column)

#define MOVE_FORWARD(ptr, cnt, type) do{ ptr += sizeof(type); cnt += sizeof(type); } while(0)
#define STEP_FORWARD(ptr, cnt, steps) do{ptr += (steps); cnt += (steps);}while(0)

#define COUT_ALIGN(x) std::cout << setiosflags(ios::left) << setw(x)
#define ENABLE_RED "\033[1;31m"
#define ENABLE_ERROR std::cout <<ENABLE_RED<< "Minisql execution error at line "<<ast->line_no_<<", col "<<ast->col_no_<<", message: "
#define DISABLED "\033[0m" <<std::endl

#endif  // MINISQL_MACROS_H
