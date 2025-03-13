#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

brew update
brew install \
  boost \
  fmt \
  spdlog \
  mariadb-connector-c\
  msgpack-cxx\
  hiredis

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
lib_install_scripts_dir=$script_dir/..
"${lib_install_scripts_dir}"/mariadb-connector-cpp.sh 1.1.5
"${lib_install_scripts_dir}"/redis-plus-plus.sh 1.3.13
