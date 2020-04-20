cmake_minimum_required(VERSION 3.16)
project(medical_sgw C)

set(CMAKE_C_STANDARD 99)

add_executable(medical_sgw main.c cmdstr.c conn_mgmt.c events_poll.c handler.c md5.c md5ops.c mt_log.c pathops.c timer_set.c)