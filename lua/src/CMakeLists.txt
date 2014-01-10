# Copyright (c) 2014 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

# add hpx_lua extension library

# Required Lua libraries
find_package(HPX_lua)
if(LUA_FOUND)
  include_directories(${LUA_INCLUDE_DIR})
  link_directories(${LUA_LIBRARY_DIR})

  find_package(HPX_luabind)
  if(LUABIND_FOUND)
    include_directories(${LUABIND_INCLUDE_DIR})
    include_directories(${HPX_script_SOURCE_DIR}/lua)

    link_directories(${LUABIND_LIBRARY_DIR})

    add_hpx_library(hpx_lua
      AUTOGLOB
      HEADER_GLOB "*.h*"
      SOURCE_GLOB "*.c*"
      FOLDER "Lua/src"
      DEPENDENCIES ${LUA_LIBRARY} ${LUABIND_LIBRARY})

  else()
    message("Luabind not found, HPX Lua bindings will not be available")
  endif()

else()
  message("Lua not found, please specify a correct LUA_ROOT to enable the Lua bindings")
endif()

