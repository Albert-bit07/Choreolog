function(choreoos_set_project_warnings target)
  if(MSVC)
    target_compile_options(
      "${target}"
      PRIVATE
        /W4
        /permissive-
        /Zc:__cplusplus
    )

    if(CHOREOOS_WARNINGS_AS_ERRORS)
      target_compile_options("${target}" PRIVATE /WX)
    endif()
  else()
    target_compile_options(
      "${target}"
      PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
    )

    if(CHOREOOS_WARNINGS_AS_ERRORS)
      target_compile_options("${target}" PRIVATE -Werror)
    endif()
  endif()
endfunction()
