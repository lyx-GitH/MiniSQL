# recursive files
FILE(GLOB_RECURSE MINISQL_SOURCE ${PROJECT_SOURCE_DIR}/src/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*.c
        ${PROJECT_SOURCE_DIR}/src/*/*/*.c
        )
ADD_LIBRARY(minisql_shared SHARED ${MINISQL_SOURCE})
TARGET_LINK_LIBRARIES(minisql_shared glog)

ADD_EXECUTABLE(main main.cpp include/common/IntervalMerge.h include/common/comparison.h)
TARGET_LINK_LIBRARIES(main glog minisql_shared)