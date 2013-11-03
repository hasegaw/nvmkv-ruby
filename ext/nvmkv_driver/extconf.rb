#!/usr/bin/env ruby
require 'mkmf'

opennvm_inc_dir, opennvm_lib_dir = dir_config('nvmkv', '/usr/include/nvm', '/usr/lib/nvm')
$LDFLAGS << " -Wl,-rpath,#{opennvm_lib_dir}"
$CFLAGS += " -std=c99"

find_header('nvm_kv.h')
find_library('nvmkv','nvm_kv_open')

have_func('rb_thread_blocking_region')

create_makefile('nvmkv_driver')
