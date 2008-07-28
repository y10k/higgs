#!/usr/local/bin/ruby

$:.unshift File.dirname(__FILE__)
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

# for ident(1)
CVS_ID = '$Id$'

require 'fileutils'
require 'rbconfig'

include Config
include FileUtils

LIB_DIR = File.join(File.dirname(__FILE__), '..', '..', 'lib')
LOOP_COUNT = ENV['LOOP_COUNT'] || '100'
DATA_COUNT = ENV['DATA_COUNT'] || '10'
MAX_DAT_LEN = ENV['MAX_DAT_LEN'] || '32768'

def run(cmd)
  ruby = CONFIG['RUBY_INSTALL_NAME']
  system ruby, '-I', LIB_DIR, cmd, LOOP_COUNT, DATA_COUNT, MAX_DAT_LEN
end

def benchmarks
  print "\n"
  yield 'dbm_seq_write.rb'
  run 'st_verify.rb'
  yield 'dbm_seq_read.rb'
  run 'st_verify.rb'
  yield 'dbm_rnd_read.rb'
  run 'st_verify.rb'
  yield 'dbm_rnd_update.rb'
  run 'st_verify.rb'
  yield 'dbm_rnd_delete.rb'
  run 'st_verify.rb'
  #yield 'st_reorganize.rb'
  #run 'st_verify.rb'
  nil
end

def clean
  for db in Dir['foo.*']
    rm_f db
  end
end

clean

benchmarks do |rb|
  run rb
end
