#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/storage'
require 'rubyunit'
require 'yaml'

module Higgs::StorageTest
  class InitOptionsTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Higgs::Storage::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
      assert_equal(2, @number_of_read_io)
      assert_equal(2, self.number_of_read_io)
      assert_equal(Higgs::Index::GDBM_OPEN[:read], @dbm_read_open)   # higgs/index/gdbm auto-required
      assert_equal(Higgs::Index::GDBM_OPEN[:write], @dbm_write_open) # higgs/index/gdbm auto-required
      assert_equal(Higgs::Cache::SharedWorkCache, @cache_type)       # higgs/cache auto-required
    end

    def test_init_options_read_only_true
      init_options(:read_only => true)
      assert_equal(true, @read_only)
      assert_equal(true, self.read_only)
    end

    def test_init_options_read_only_false
      init_options(:read_only => false)
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
    end

    def test_init_options_number_of_read_io
      init_options(:number_of_read_io => 16)
      assert_equal(16, @number_of_read_io)
      assert_equal(16, self.number_of_read_io)
    end

    def test_init_options_dbm_open
      init_options(:dbm_open => { :read => :dummy_read_open, :write => :dummy_write_open })
      assert_equal(:dummy_read_open, @dbm_read_open)
      assert_equal(:dummy_write_open, @dbm_write_open)
    end

    def test_init_options_cache_type
      init_options(:cache_type => :dummy_cache_type)
      assert_equal(:dummy_cache_type, @cache_type)
    end
  end
end
