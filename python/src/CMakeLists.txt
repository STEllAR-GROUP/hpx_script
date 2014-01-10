# Copyright (c) 2014 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

# add hpx_python extension library

# Required Python libraries
find_package(PythonLibs)
if(PYTHONLIBS_FOUND)
  include_directories(${PYTHON_INCLUDE_DIRS})

  # Make sure the Boost.Python Package is available
  set(BOOST_LIBRARIES python)
  find_package(HPX_Boost)

  if(NOT (BOOST_PYTHON_LIBRARY STREQUAL BOOST_PYTHON_LIBRARY-NOTFOUND))
    include_directories(${BOOST_INCLUDE_DIR})
    include_directories(${HPX_script_SOURCE_DIR}/python)

    # Add the module
    add_hpx_library(hpx_python
      AUTOGLOB
      HEADER_GLOB "*.h*"
      SOURCE_GLOB "*.c*"
      FOLDER "Python/src"
      DEPENDENCIES ${PYTHON_LIBRARY} ${BOOST_PYTHON_LIBRARY})

  else()
    hpx_warning("hpx_script.python" "Boost.Python not found, HPX Python bindings will not be available")
  endif()

else()
  hpx_warning("hpx_script.python" "Python not found, please specify a correct PYTHON_ROOT to enable the Python bindings")
endif()

