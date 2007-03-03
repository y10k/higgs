#!/usr/local/bin/ruby

require 'higgs/storage'
require 'rubyunit'

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
      assert_instance_of(Higgs::Cache::LRUCache, @properties_cache)  # higgs/cache auto-required
      assert_equal(true, @fsync)
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

    def test_init_options_properties_cache
      init_options(:properties_cache => :dummy_cache)
      assert_equal(:dummy_cache, @properties_cache)
    end

    def test_init_options_fsync_true
      init_options(:fsync => true)
      assert_equal(true, @fsync)
    end

    def test_init_options_fsync_false
      init_options(:fsync => false)
      assert_equal(false, @fsync)
    end
  end
end
