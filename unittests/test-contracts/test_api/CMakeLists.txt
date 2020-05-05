
#find_package(Boost 1.67 REQUIRED COMPONENTS program_options unit_test_framework)
message(STATUS "test_api ***************** Boost_FOUND = ${Boost_FOUND}, Boost_INCLUDE_DIRS=${Boost_INCLUDE_DIRS} Boost_LIBRARY_DIRS=${Boost_LIBRARY_DIRS}")

if( EOSIO_COMPILE_TEST_CONTRACTS )
   add_executable( test_api test_api.cpp )

target_include_directories( test_api
       PUBLIC
       ${CMAKE_CURRENT_SOURCE_DIR}/../../../libraries/chain/include
       ${CMAKE_CURRENT_SOURCE_DIR}/../../../libraries/fc/include
       ${Boost_INCLUDE_DIRS}
   )

#   target_link_directories( test_api
#     PUBLIC
#     Boost::boost
#   )
   
else()
   configure_file( ${CMAKE_CURRENT_SOURCE_DIR}/test_api.wasm ${CMAKE_CURRENT_BINARY_DIR}/test_api.wasm COPYONLY )
endif()
