#!/usr/local/bin/ruby

require 'digest/sha2'
require 'fileutils'
require 'higgs/storage'
require 'rubyunit'
require 'yaml'

module Higgs::StorageTest
  # for ident(1)
  CVS_ID = '$Id$'

  class InitOptionsTest < RUNIT::TestCase
    include Higgs::Storage::InitOptions

    def test_init_options_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(2, @number_of_read_io)
      assert_equal(Higgs::Index::GDBM_OPEN[:read], @dbm_read_open)   # higgs/index/gdbm auto-required
      assert_equal(Higgs::Index::GDBM_OPEN[:write], @dbm_write_open) # higgs/index/gdbm auto-required
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
  end

  class StorageTest < RUNIT::TestCase
    def setup
      @tmp_dir = 'storage_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'storage_test')
      @s = Higgs::Storage.new(@name)
    end

    def teardown
      @s.shutdown
      FileUtils.rm_rf(@tmp_dir)
    end

    def test_fetch
      info_yml = @s.fetch('.higgs')
      assert_not_nil(info_yml)
      info = YAML.load(info_yml)
      assert_instance_of(Hash, info)
      assert_equal(0, info['version']['major'])
      assert_equal(0, info['version']['minor'])
      assert_match(info['cvs_id'], /^\$Id/)
      assert_instance_of(Time, info['build_time'])
      assert_equal('SHA512', info['hash_type'])
    end

    def test_fetch_properties
      info_yml = @s.fetch('.higgs')
      assert_not_nil(info_yml)
      properties = @s.fetch_properties('.higgs')
      assert_equal(Digest::SHA512.hexdigest(info_yml), properties['hash'])
      assert_instance_of(Time, properties['created_time'])
      assert_equal({}, properties['custom_properties'])
    end
  end
end
