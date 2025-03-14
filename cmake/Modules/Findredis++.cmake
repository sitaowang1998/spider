# Try to find redis++
#
# Set redis++_USE_STATIC_LIBS=ON to look for static libraries.
#
# Once done this will define:
#  redis++_FOUND - Whether redis++ was found on the system
#  redis++_INCLUDE_DIR - The redis++ include directories
#  redis++_VERSION - The version of redis++ installed on the system
#
# Conventions:
# - Variables only for use within the script are prefixed with "REDIS++_"
# - Variables that should be externally visible are prefixed with "redis++_"

set(REDIS++_LIBNAME "redis++")
set(REDIS++_TARGET_NAME "redis++::redis++")

include(cmake/Modules/FindLibraryDependencies.cmake)

# Run pkg-config
find_package(PkgConfig)
pkg_check_modules(REDIS++_PKGCONF QUIET ${REDIS++_LIBNAME})

# Set the include directories
find_path(
    redis++_INCLUDE_DIR
    redis++/redis++.h
    HINTS
        ${REDIS++_PKGCONF_INCLUDE_DIRS}
    PATH_SUFFIXES
        sw
)

# Handle static libraries
if(redis++_USE_STATIC_LIBS)
    # Save current value of CMAKE_FIND_LIBRARY_SUFFIXES
    set(REDIS++_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})

    # Temporarily change CMAKE_FIND_LIBRARY_SUFFIXES to static library suffix
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
endif()

# Find the library
# Find library
find_library(
    REDIS++_LIBRARY
    NAMES
        ${REDIS++_LIBNAME}
    HINTS
        ${REDIS++_PKGCONF_LIBDIR}
    PATH_SUFFIXES
        lib
)
if(REDIS++_LIBRARY)
    # NOTE: This must be set for find_package_handle_standard_args to work
    set(redis++_FOUND ON)
endif()

if(redis++_USE_STATIC_LIBS)
    findstaticlibrarydependencies(${REDIS++_LIBNAME} REDIS++ "${REDIS++_PKGCONF_STATIC_LIBRARIES}")
    # Restore original value of CMAKE_FIND_LIBRARY_SUFFIXES
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${REDIS++_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
    unset(REDIS++_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES)
else()
    finddynamiclibrarydependencies(REDIS++ "${REDIS++_PKGCONF_LIBRARIES}")
endif()

# Set version
set(redis++_VERSION ${REDIS++_PKGCONF_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    ${REDIS++_LIBNAME}
    REQUIRED_VARS
        redis++_INCLUDE_DIR
    VERSION_VAR redis++_VERSION
)

if(NOT TARGET ${REDIS++_TARGET_NAME})
    # Add library to build
    if(redis++_FOUND)
        if(redis++_USE_STATIC_LIBS)
            add_library(${REDIS++_TARGET_NAME} STATIC IMPORTED)
            set_target_properties(
                ${REDIS++_TARGET_NAME}
                PROPERTIES
                    COMPILE_FLAGS
                        "${REDIS++_PKGCONF_STATIC_CFLAGS}"
            )
        else()
            # NOTE: We use UNKNOWN so that if the user doesn't have the SHARED
            # libraries installed, we can still use the STATIC libraries.
            add_library(${REDIS++_TARGET_NAME} UNKNOWN IMPORTED)
            set_target_properties(
                ${REDIS++_TARGET_NAME}
                PROPERTIES
                    COMPILE_FLAGS
                        "${REDIS++_PKGCONF_CFLAGS}"
            )
        endif()
    endif()

    # Set include directories for library
    if(NOT EXISTS "${redis++_INCLUDE_DIR}")
        set(redis++_FOUND OFF)
    else()
        set_target_properties(
            ${REDIS++_TARGET_NAME}
            PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES
                    "${redis++_INCLUDE_DIR}"
        )
    endif()

    # Set location of library
    if(NOT EXISTS "${REDIS++_LIBRARY}")
        set(redis++_FOUND OFF)
    else()
        set_target_properties(
            ${REDIS++_TARGET_NAME}
            PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES
                    "CXX"
                IMPORTED_LOCATION
                    "${REDIS++_LIBRARY}"
        )

        # Add component's dependencies for linking
        if(REDIS++_LIBRARY_DEPENDENCIES)
            set_target_properties(
                ${REDIS++_TARGET_NAME}
                PROPERTIES
                    INTERFACE_LINK_LIBRARIES
                        "${REDIS++_LIBRARY_DEPENDENCIES}"
            )
        endif()
    endif()
endif()
