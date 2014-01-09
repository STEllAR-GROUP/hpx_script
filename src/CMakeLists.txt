# Copyright (c) 2007-2014 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

# add hpx_lua extension library

add_hpx_library(hpx_lua
  AUTOGLOB
  HEADER_GLOB "*.h*"
  SOURCE_GLOB "*.c*"
  FOLDER "hpx_lua/src"
  DEPENDENCIES ${LUA_LIBRARY} ${LUABIND_LIBRARY})
