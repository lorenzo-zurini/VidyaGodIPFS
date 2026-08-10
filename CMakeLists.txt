cmake_minimum_required(VERSION 3.16)
project(vidyagodipfs LANGUAGES NONE)

# Builds the embedded Boxo IPFS node as an in-process c-shared library (libvgipfs.so + libvgipfs.h) that
# VidyaGod links and calls via cgo. Output lands in the top build dir next to the VidyaGod binary so the app
# resolves it through an $ORIGIN rpath and the AppImage step bundles it (same co-location idea as vidyagodfs).

find_program(GO_EXECUTABLE go REQUIRED)

# c-shared output: libvgipfs.so on Linux, a DLL on Windows. Keep the "libvgipfs" basename on both so the
# cgo-generated header is libvgipfs.h either way (go names the header after the -o basename).
if(WIN32)
    set(VGIPFS_LIB ${CMAKE_BINARY_DIR}/libvgipfs.dll)
else()
    set(VGIPFS_LIB ${CMAKE_BINARY_DIR}/libvgipfs.so)
endif()
set(VGIPFS_HDR ${CMAKE_BINARY_DIR}/libvgipfs.h)

file(GLOB VGIPFS_SRCS CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.go)

# MSYS2's Go binary is trimmed and needs GOROOT in the env (not embedded). Derive it from the go
# executable's location (<prefix>/bin/go -> <prefix>/lib/go) and pass it into the build command.
set(VGIPFS_GO_ENV CGO_ENABLED=1)
if(WIN32)
    get_filename_component(_go_bindir ${GO_EXECUTABLE} DIRECTORY)
    get_filename_component(_go_prefix ${_go_bindir} DIRECTORY)
    list(APPEND VGIPFS_GO_ENV GOROOT=${_go_prefix}/lib/go CC=gcc)
endif()

add_custom_command(
    OUTPUT ${VGIPFS_LIB} ${VGIPFS_HDR}
    COMMAND ${CMAKE_COMMAND} -E env ${VGIPFS_GO_ENV}
            ${GO_EXECUTABLE} build -buildmode=c-shared -o ${VGIPFS_LIB} .
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${VGIPFS_SRCS} ${CMAKE_CURRENT_SOURCE_DIR}/go.mod
    COMMENT "Building the Boxo embedded IPFS node — go build -buildmode=c-shared"
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
if(WIN32)
    # go's c-shared build emits only the DLL + header (no separate import lib); MinGW ld links a DLL
    # directly, so point IMPORTED_IMPLIB at the DLL itself. The DLL sits next to VidyaGod.exe at runtime.
    set_target_properties(vgipfs PROPERTIES IMPORTED_IMPLIB ${VGIPFS_LIB})
endif()
