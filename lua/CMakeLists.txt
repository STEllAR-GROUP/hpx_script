# Copyright (c) 2014 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

# Required Lua libraries
find_package(HPX_lua)
if(LUA_FOUND)
  include_directories(${LUA_INCLUDE_DIR})
  link_directories(${LUA_LIBRARY_DIR})
else()
  message(FATAL " Lua not found, please specify a correct LUA_ROOT!")
endif()

find_package(HPX_luabind)
if(LUABIND_FOUND)
  include_directories(${LUABIND_INCLUDE_DIR})
  link_directories(${LUABIND_LIBRARY_DIR})
else()
  message(FATAL " Luabind not found, please specify a correct LUABIND_ROOT!")
endif()

# Adjust auxiliary build settings for HPX to work properly
if(NOT MSVC)
  set(HPX_RPATH ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}${HPX_RPATH})
else()
  # enable solution folders for MSVC
  set_property(GLOBAL PROPERTY USE_FOLDERS ON)
  if(HPX_NO_INSTALL)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/$(Configuration)" CACHE PATH
      "Prefix prepended to install directories." FORCE)
  endif()
endif()

include_directories(${HPX_script_SOURCE_DIR}/lua)

add_subdirectory(src)

