cmake_minimum_required(VERSION 3.16)
project(vidyagodipfs LANGUAGES NONE)

# Builds the embedded Boxo IPFS node as an in-process c-shared library (libvgipfs.so + libvgipfs.h) that
# VidyaGod links and calls via cgo. Output lands in the top build dir next to the VidyaGod binary so the app
# resolves it through an $ORIGIN rpath and the AppImage step bundles it (same co-location idea as vidyagodfs).

find_program(GO_EXECUTABLE go REQUIRED)

set(VGIPFS_LIB ${CMAKE_BINARY_DIR}/libvgipfs.so)
set(VGIPFS_HDR ${CMAKE_BINARY_DIR}/libvgipfs.h)

file(GLOB VGIPFS_SRCS CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.go)

add_custom_command(
    OUTPUT ${VGIPFS_LIB} ${VGIPFS_HDR}
    COMMAND ${CMAKE_COMMAND} -E env CGO_ENABLED=1
            ${GO_EXECUTABLE} build -buildmode=c-shared -o ${VGIPFS_LIB} .
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${VGIPFS_SRCS} ${CMAKE_CURRENT_SOURCE_DIR}/go.mod
    COMMENT "Building libvgipfs.so (Boxo embedded IPFS node) — go build -buildmode=c-shared"
    VERBATIM
)

add_custom_target(vgipfs_build ALL DEPENDS ${VGIPFS_LIB})

# An imported shared lib the parent links against; the include dir carries the cgo-generated header.
add_library(vgipfs SHARED IMPORTED GLOBAL)
add_dependencies(vgipfs vgipfs_build)
set_target_properties(vgipfs PROPERTIES
    IMPORTED_LOCATION ${VGIPFS_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}
)
