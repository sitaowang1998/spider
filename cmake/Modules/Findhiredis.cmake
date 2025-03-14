# Try to find hiredis
#
# Set hiredis_USE_STATIC_LIBS=ON to look for static libraries.
#
# Once done this will define:
#  hiredis_FOUND - Whether hiredis was found on the system
#  hiredis_INCLUDE_DIR - The hiredis include directories
#  hiredis_VERSION - The version of hiredis installed on the system
#
# Conventions:
# - Variables only for use within the script are prefixed with "HIREDIS_"
# - Variables that should be externally visible are prefixed with "hiredis_"

set(HIREDIS_LIBNAME "hiredis")
set(HIREDIS_TARGET_NAME "hiredis::hiredis")

include(cmake/Modules/FindLibraryDependencies.cmake)

# Run pkg-config
find_package(PkgConfig)
pkg_check_modules(HIREDIS_PKGCONF QUIET ${HIREDIS_LIBNAME})

# Set the include directories
find_path(hiredis_INCLUDE_DIR hiredis.h HINTS ${HIREDIS_PKGCONF_INCLUDE_DIRS} PATH_SUFFIXES hiredis)

# Handle static libraries
if(hiredis_USE_STATIC_LIBS)
    # Save current value of CMAKE_FIND_LIBRARY_SUFFIXES
    set(HIREDIS_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})

    # Temporarily change CMAKE_FIND_LIBRARY_SUFFIXES to static library suffix
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
endif()

# Find the library
# Find library
find_library(
    HIREDIS_LIBRARY
    NAMES
        ${HIREDIS_LIBNAME}
    HINTS
        ${HIREDIS_PKGCONF_LIBDIR}
    PATH_SUFFIXES
        lib
)
if(HIREDIS_LIBRARY)
    # NOTE: This must be set for find_package_handle_standard_args to work
    set(hiredis_FOUND ON)
endif()

if(hiredis_USE_STATIC_LIBS)
    findstaticlibrarydependencies(${HIREDIS_LIBNAME} HIREDIS "${HIREDIS_PKGCONF_STATIC_LIBRARIES}")
    # Restore original value of CMAKE_FIND_LIBRARY_SUFFIXES
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${HIREDIS_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
    unset(HIREDIS_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES)
else()
    finddynamiclibrarydependencies(HIREDIS "${HIREDIS_PKGCONF_LIBRARIES}")
endif()

# Set version
set(hiredis_VERSION ${HIREDIS_PKGCONF_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    ${HIREDIS_LIBNAME}
    REQUIRED_VARS
        hiredis_INCLUDE_DIR
    VERSION_VAR hiredis_VERSION
)

if(NOT TARGET ${HIREDIS_TARGET_NAME})
    # Add library to build
    if(hiredis_FOUND)
        if(hiredis_USE_STATIC_LIBS)
            add_library(${HIREDIS_TARGET_NAME} STATIC IMPORTED)
            set_target_properties(
                ${HIREDIS_TARGET_NAME}
                PROPERTIES
                    COMPILE_FLAGS
                        "${HIREDIS_PKGCONF_STATIC_CFLAGS}"
            )
        else()
            # NOTE: We use UNKNOWN so that if the user doesn't have the SHARED
            # libraries installed, we can still use the STATIC libraries.
            add_library(${HIREDIS_TARGET_NAME} UNKNOWN IMPORTED)
            set_target_properties(
                ${HIREDIS_TARGET_NAME}
                PROPERTIES
                    COMPILE_FLAGS
                        "${HIREDIS_PKGCONF_CFLAGS}"
            )
        endif()
    endif()

    # Set include directories for library
    if(NOT EXISTS "${hiredis_INCLUDE_DIR}")
        set(hiredis_FOUND OFF)
    else()
        set_target_properties(
            ${HIREDIS_TARGET_NAME}
            PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES
                    "${hiredis_INCLUDE_DIR}"
        )
    endif()

    # Set location of library
    if(NOT EXISTS "${HIREDIS_LIBRARY}")
        set(hiredis_FOUND OFF)
    else()
        set_target_properties(
            ${HIREDIS_TARGET_NAME}
            PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES
                    "C"
                IMPORTED_LOCATION
                    "${HIREDIS_LIBRARY}"
        )

        # Add component's dependencies for linking
        if(HIREDIS_LIBRARY_DEPENDENCIES)
            set_target_properties(
                ${HIREDIS_TARGET_NAME}
                PROPERTIES
                    INTERFACE_LINK_LIBRARIES
                        "${HIREDIS_LIBRARY_DEPENDENCIES}"
            )
        endif()
    endif()
endif()
