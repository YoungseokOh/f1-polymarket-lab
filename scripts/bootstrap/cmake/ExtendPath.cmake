function(extend_path out_var base_path)
  set(resolved_path "${base_path}")

  foreach(path_part IN LISTS ARGN)
    if("${path_part}" STREQUAL "")
      continue()
    endif()

    if(IS_ABSOLUTE "${path_part}")
      set(resolved_path "${path_part}")
    else()
      set(resolved_path "${resolved_path}/${path_part}")
    endif()
  endforeach()

  file(TO_CMAKE_PATH "${resolved_path}" resolved_path)
  set(${out_var} "${resolved_path}" PARENT_SCOPE)
endfunction()
