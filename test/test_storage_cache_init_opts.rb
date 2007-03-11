#!/usr/local/bin/ruby

require 'higgs/storage'
require 'rubyunit'

module Higgs::StorageTest
  class StorageCacheManagerInitOptionsTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Higgs::Storage::CacheManager::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(Higgs::Storage, @source_storage_type)
      assert_instance_of(Proc, @read)
      assert_equal('HALO', @read.call('HALO'))
      assert_instance_of(Proc, @write)
      assert_equal('HALO', @write.call('HALO'))
      assert_instance_of(Higgs::Cache::LRUCache, @read_cache) # higgs/cache auto-require
    end

    def test_source_storage_type
      init_options(:source_storage_type => :dummy_storage_type)
      assert_equal(:dummy_storage_type, @source_storage_type)
    end

    def test_io_conversion
      init_options(:io_conversion => { :read => :dummy_read, :write => :dummy_write })
      assert_equal(:dummy_read, @read)
      assert_equal(:dummy_write, @write)
    end

    def test_io_conversion_error_required_read_procedure
      assert_exception(RuntimeError) {
	init_options(:io_conversion => { :write => :dummy_write })
      }
    end

    def test_io_conversion_error_required_write_procedure
      assert_exception(RuntimeError) {
	init_options(:io_conversion => { :read => :dummy_read })
      }
    end

    def test_init_options_read_cache
      init_options(:read_cache => :dummy_cache)
      assert_equal(:dummy_cache, @read_cache)
    end
  end
end
