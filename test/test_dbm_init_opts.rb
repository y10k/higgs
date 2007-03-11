#!/usr/local/bin/ruby

require 'higgs/dbm'
require 'rubyunit'

module Higgs::DBMTest
  class InitOptionsTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Higgs::DBM::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
      assert_equal(Higgs::Storage::CacheManager, @storage_type) # higss/storage auto-require
      assert_instance_of(Higgs::Lock::FineGrainLockManager, @lock_manager) # higgs/lock auto-require
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

    def test_init_options_storage_type
      init_options(:storage_type => :dummy_storage_type)
      assert_equal(:dummy_storage_type, @storage_type)
    end

    def test_init_options_lock_manager
      init_options(:lock_manager => :dummy_lock_manager)
      assert_equal(:dummy_lock_manager, @lock_manager)
    end
  end
end
