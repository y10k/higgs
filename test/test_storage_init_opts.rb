#!/usr/local/bin/ruby

require 'higgs/storage'
require 'test/unit'

module Higgs::Test
  class StorageInitOptionsTest < Test::Unit::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Higgs::Storage::InitOptions

    def test_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
      assert_equal(2, @number_of_read_io)
      assert_equal(2, self.number_of_read_io)
      assert_instance_of(Higgs::LRUCache, @properties_cache) # auto: require 'higgs/cache'
      assert_equal(false, @jlog_sync)
      assert_equal(false, self.jlog_sync)
      assert_equal(1024 * 256, @jlog_rotate_size)
      assert_equal(1024 * 256, self.jlog_rotate_size)
      assert_equal(1, @jlog_rotate_max)
      assert_equal(1, self.jlog_rotate_max)
      assert_equal(nil, @jlog_rotate_service_uri)
      assert_equal(nil, self.jlog_rotate_service_uri)
    end

    def test_read_only_true
      init_options(:read_only => true)
      assert_equal(true, @read_only)
      assert_equal(true, self.read_only)
    end

    def test_read_only_false
      init_options(:read_only => false)
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
    end

    def test_number_of_read_io
      init_options(:number_of_read_io => 16)
      assert_equal(16, @number_of_read_io)
      assert_equal(16, self.number_of_read_io)
    end

    def test_properties_cache
      init_options(:properties_cache => :dummy_cache)
      assert_equal(:dummy_cache, @properties_cache)
    end

    def test_jlog_sync_true
      init_options(:jlog_sync => true)
      assert_equal(true, @jlog_sync)
      assert_equal(true, self.jlog_sync)
    end

    def test_jlog_sync_false
      init_options(:jlog_sync => false)
      assert_equal(false, @jlog_sync)
      assert_equal(false, self.jlog_sync)
    end

    def test_jlog_rotate_size
      init_options(:jlog_rotate_size => 1024**2)
      assert_equal(1024**2, @jlog_rotate_size)
      assert_equal(1024**2, self.jlog_rotate_size)
    end

    def test_jlog_rotate_max
      init_options(:jlog_rotate_max => 100)
      assert_equal(100, @jlog_rotate_max)
      assert_equal(100, self.jlog_rotate_max)
    end

    def test_jlog_rotate_service_uri
      init_options(:jlog_rotate_service_uri => 'druby://localhost:14142')
      assert_equal('druby://localhost:14142', @jlog_rotate_service_uri)
      assert_equal('druby://localhost:14142', self.jlog_rotate_service_uri)
    end
  end
end
