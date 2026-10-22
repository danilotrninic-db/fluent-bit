# The Zerobus pure C SDK is not vendored: it is compiled from an SDK checkout.
# Point FLB_ZEROBUS_SDK_PATH at its purec/ directory.
set(FLB_ZEROBUS_SDK_PATH "" CACHE PATH "Path to the Zerobus pure C SDK purec/ directory")

if(NOT EXISTS "${FLB_ZEROBUS_SDK_PATH}/include/zerobus/zerobus.h")
  message(FATAL_ERROR
    "out_zerobus_stream: set FLB_ZEROBUS_SDK_PATH to the SDK's purec/ directory")
endif()
if(NOT OPENSSL_FOUND)
  message(FATAL_ERROR "out_zerobus_stream requires OpenSSL")
endif()
find_package(Threads REQUIRED)

# Compile the SDK against Fluent Bit's bundled nghttp2 and protobuf-c runtime
# instead of the system copies its own build looks for, so the binary carries
# one copy of each library.
file(GLOB_RECURSE ZEROBUS_SDK_SOURCES CONFIGURE_DEPENDS
  "${FLB_ZEROBUS_SDK_PATH}/src/*.c"
  "${FLB_ZEROBUS_SDK_PATH}/proto/generated/*.pb-c.c")

# As in the SDK's build: protoc-gen-c emits a union initializer GCC warns about
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  set_source_files_properties(
    "${FLB_ZEROBUS_SDK_PATH}/proto/generated/zerobus_service.pb-c.c"
    PROPERTIES COMPILE_OPTIONS -Wno-missing-braces)
endif()

add_library(zerobus-purec STATIC ${ZEROBUS_SDK_SOURCES})
target_include_directories(zerobus-purec
  PUBLIC  "${FLB_ZEROBUS_SDK_PATH}/include"
  PRIVATE "${FLB_ZEROBUS_SDK_PATH}/src"
          "${FLB_ZEROBUS_SDK_PATH}/proto/generated"
          "${FLB_PATH_ROOT_SOURCE}/${FLB_PATH_LIB_FLUENT_OTEL}/proto_c")
target_compile_features(zerobus-purec PRIVATE c_std_11)
target_link_libraries(zerobus-purec
  PUBLIC ${NGHTTP2_LIBRARIES} fluent-otel-proto
         OpenSSL::SSL OpenSSL::Crypto Threads::Threads)

set(src
  zerobus_stream.c
  )

FLB_PLUGIN(out_zerobus_stream "${src}" "zerobus-purec")
