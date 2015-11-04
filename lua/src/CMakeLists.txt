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

find_package(Readline)
if(READLINE_FOUND)
  add_definitions(-DLUA_USE_READLINE)
endif()
find_package(HPX)
if(HPX_FOUND)
  include_directories(${HPX_INCLUDE_DIRS})
  link_directories(${HPX_LIB_DIRS})

  include_directories(${LUA_DIR}/include)
  link_directories(${LUA_DIR}/lib)

  include_directories(.)


  add_hpx_executable(xlua
    ESSENTIAL
    SOURCES lua.cpp
    DEPENDENCIES xlua_lib
    )

  add_hpx_library(xlua
    SOURCES xlua.cpp counter.cpp table.cpp vector.cpp component.cpp apex.cpp
    HEADERS xlua.hpp
  )

  add_hpx_executable(hello
    ESSENTIAL
    SOURCES examples/hello.cpp 
    DEPENDENCIES xlua_lib
    )

  if(READLINE_FOUND)
    target_link_libraries(xlua_exe lua readline)
  else()
    target_link_libraries(xlua_exe lua)
  endif()

  target_link_libraries(hello_exe lua)
else()
  message("Could not find HPX.")
endif()
