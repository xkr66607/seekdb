# Focused native coverage for the competition's asynchronous AI columns.
# The production build excludes this target unless it is requested explicitly.
add_executable(ai_column_tests EXCLUDE_FROM_ALL
  share/schema/test_ai_column_schema.cpp
  sql/parser/test_ai_column_parser.cpp)
target_link_libraries(ai_column_tests PRIVATE
  ob_base
  -Wl,--start-group
  ob_malloc
  oceanbase_static
  ob_sql_static
  ob_storage_static
  ob_share_static
  sql_nio
  ${DEP_DIR}/lib/libgtest_main.a
  ${DEP_DIR}/lib/libgtest.a
  -Wl,--end-group
  ${link_malloc_hook}
  -static-libgcc
  -static-libstdc++)
add_test(NAME ai_column_tests COMMAND ai_column_tests)
