function(choreoos_enable_sanitizers target)
  if(NOT CHOREOOS_ENABLE_SANITIZERS)
    return()
  endif()

  if(MSVC)
    message(FATAL_ERROR "The sanitizer preset is currently supported on GCC and Clang only.")
  endif()

  target_compile_options(
    "${target}"
    PRIVATE
      -fsanitize=address,undefined
      -fno-omit-frame-pointer
  )
  target_link_options("${target}" PRIVATE -fsanitize=address,undefined)
endfunction()
