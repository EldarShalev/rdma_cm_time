cmake_minimum_required(VERSION 3.14)
project(rdma_cm_time C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -lrdmacm -libverbs -lpthread -D _GNU_SOURCE " )

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

include_directories(/labhome/eldarsh/workspace/rdma-core/cmake-build-debug/include)
include_directories(/labhome/eldarsh/workspace/rdma-core/cmake-build-debug/lib)
include_directories(/labhome/eldarsh/workspace/rdma-core/librdmacm)
include_directories(/usr/include)



add_executable(rdma_cm_time main.c queue.c queue.h queue_internal.c queue_internal.h threadpool.c threadpool.h ibv_helper.h)

