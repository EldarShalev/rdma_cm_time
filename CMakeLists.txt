cmake_minimum_required(VERSION 3.14)
project(rdma_cm_time C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -lrdmacm -libverbs -lpthread " )

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

add_executable(rdma_cm_time main.c queue.c queue.h queue_internal.c queue_internal.h threadpool.c threadpool.h)

