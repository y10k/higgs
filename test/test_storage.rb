#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/storage'
require 'rubyunit'

module Higgs::StorageTest
  # for ident(1)
  CVS_ID = '$Id$'

  class InitOptionsTest < RUNIT::TestCase
    include Higgs::Storage::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(2, @number_of_read_io)
      assert_equal(Higgs::Index::GDBM_OPEN[:read], @dbm_read_open)         # higgs/index/gdbm auto-required
      assert_equal(Higgs::Index::GDBM_OPEN[:write], @dbm_write_open)       # higgs/index/gdbm auto-required
      assert_instance_of(Higgs::Lock::FineGrainLockManager, @lock_manager) # higgs/lock auto-required
    end

    def test_init_options_read_only_true
      init_options(:read_only => true)
      assert_equal(true, @read_only)
    end

    def test_init_options_read_only_false
      init_options(:read_only => false)
      assert_equal(false, @read_only)
    end

    def test_init_options_dbm_open
      init_options(:dbm_open => { :read => :dummy_read_open, :write => :dummy_write_open })
      assert_equal(:dummy_read_open, @dbm_read_open)
      assert_equal(:dummy_write_open, @dbm_write_open)
    end

    def test_init_options_lock_manager
      init_options(:lock_manager => :dummy_lock_manager)
      assert_equal(:dummy_lock_manager, @lock_manager)
    end
  end

  class StorageTest < RUNIT::TestCase
    def setup
      @tmp_dir = 'storage_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'storage_test')
    end

    def teardown
      FileUtils.rm_rf(@tmp_dir)
    end

    def test_storage
      s = Higgs::Storage.new(@name)
      s.shutdown
    end
  end
end
