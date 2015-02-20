# Copyright (c) 2014 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 2.8.4 FATAL_ERROR)

project(xlua CXX)

if(NOT HPX_ROOT)
  message(FATAL "HPX_ROOT not set, unable to find HPX!")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
  ${HPX_ROOT}/share/cmake-${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}/Modules)

find_package(HPX)
if(HPX_FOUND)
  include_directories(${HPX_INCLUDE_DIRS})
  link_directories(${HPX_LIB_DIRS})

  include_directories(${LUA_DIR}/src)
  link_directories(${LUA_DIR}/src)

  add_definitions(-DLUA_USE_READLINE)

  include_directories(.)

  add_library(xlua xlua.cpp)

  add_hpx_executable(xlua
    ESSENTIAL
    SOURCES lua.cpp)

  add_hpx_executable(hello
    ESSENTIAL
    SOURCES examples/hello.cpp)

  target_link_libraries(xlua_exe xlua lua readline)
  target_link_libraries(hello_exe xlua lua)
else()
  message("Could not find HPX.")
endif()
