# Copyright (c) 2014 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

if(NOT HPX_FINDPACKAGE_LOADED)
  include(HPX_FindPackage)
endif()

hpx_find_package(LUABIND
  LIBRARIES luabind luabindd
  LIBRARY_PATHS stage64/lib
  HEADERS luabind/luabind.hpp
  HEADER_PATHS .)

