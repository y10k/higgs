#!/usr/local/bin/ruby

require 'digest/sha2'
require 'fileutils'
require 'higgs/storage'
require 'higgs/tar'
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

  class StorageTest < RUNIT::TestCase
    include Higgs::Tar::Block

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

    def test_storage_information_fetch
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

    def test_storage_information_fetch_properties
      info_yml = @s.fetch('.higgs')
      assert_not_nil(info_yml)
      properties = @s.fetch_properties('.higgs')
      assert_equal(Digest::SHA512.hexdigest(info_yml), properties['hash'])
      assert_instance_of(Time, properties['created_time'])
      assert_equal({}, properties['custom_properties'])
    end

    def test_reopen
      @s.shutdown
      @s = Higgs::Storage.new(@name)
      test_storage_information_fetch
      test_storage_information_fetch_properties
    end

    def test_fetch_TypeError_key_not_string
      assert_exception(TypeError) { @s.fetch(:foo) }
    end

    def test_fetch_properties_TypeError_key_not_string
      assert_exception(TypeError) { @s.fetch_properties(:foo) }
    end

    def test_write_and_commit_fetch
      assert_nil(@s.fetch('foo'))
      assert_nil(@s.fetch_properties('foo'))

      # add
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])

      assert_equal("Hello world.\n", @s.fetch('foo'))
      properties = @s.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Hello world.\n"), properties['hash'])
      assert_equal({}, properties['custom_properties'])

      # update properties
      @s.write_and_commit([ [ 'foo', :update_properties, { :comment => 'test' } ] ])

      assert_equal("Hello world.\n", @s.fetch('foo'))
      properties = @s.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Hello world.\n"), properties['hash'])
      assert_equal({ :comment => 'test' }, properties['custom_properties'])

      # update
      @s.write_and_commit([ [ 'foo', :write, "Good bye.\n" ] ])

      assert_equal("Good bye.\n", @s.fetch('foo'))
      properties = @s.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Good bye.\n"), properties['hash'])
      assert_equal({ :comment => 'test' }, properties['custom_properties'])

      # delete
      @s.write_and_commit([ [ 'foo', :delete ] ])

      assert_nil(@s.fetch('foo'))
      assert_nil(@s.fetch_properties('foo'))
    end

    def test_write_and_commit_fetch_zero_bytes
      assert_nil(@s.fetch('foo'))
      assert_nil(@s.fetch_properties('foo'))

      @s.write_and_commit([ [ 'foo', :write, '' ] ])

      assert_equal('', @s.fetch('foo'))
      properties = @s.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest(''), properties['hash'])
      assert_equal({}, properties['custom_properties'])
    end

    def test_write_and_commit_read_only_NotWritableError
      @s.shutdown
      @s = Higgs::Storage.new(@name, :read_only => true)
      assert_exception(Higgs::Storage::NotWritableError) {
        @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      }
    end

    def test_write_and_commit_KeyError_not_exist_properties
      # KeyError : ruby 1.9 feature
      assert_exception((defined? KeyError) ? KeyError : IndexError) {
        @s.write_and_commit([ [ 'foo', :update_properties, {} ] ])
      }
    end

    def test_write_and_commit_TypeError_key_not_string
      assert_exception(TypeError) {
        @s.write_and_commit([ [ :foo, :write, "Hello world.\n" ] ])
      }
    end

    def test_write_and_commit_TypeError_value_not_string
      assert_exception(TypeError) {
        @s.write_and_commit([ [ 'foo', :write, "Hello world.\n".to_sym ] ])
      }
    end

    def test_write_and_commit_ArgumentError_operation_unknown
      assert_exception(ArgumentError) {
        @s.write_and_commit([ [ 'foo', :unknown, "Hello world.\n" ] ])
      }
    end

    def test_rollback_before_rollback_log_write
      @s.write_and_commit([ [ 'foo', :write, 'first' ],
                            [ 'bar', :write, 'second' ]
                          ])

      assert_exception(Higgs::Storage::DebugRollbackBeforeRollbackLogWriteException) {
        @s.write_and_commit([ [ 'foo', :write, 'third' ],
                              [ 'foo', :update_properties, { :comment => 'Hello world.' } ],
                              [ 'bar', :delete ],
                              [ 'baz', :write, 'fourth' ],
                              [ 'nop', :__debug_rollback_before_rollback_log_write__ ]
                            ])
      }

      assert_equal('first', @s.fetch('foo'))
      assert_equal({}, @s.fetch_properties('foo')['custom_properties'])
      assert_equal('second', @s.fetch('bar'))
      assert_nil(@s.fetch('baz'))
    end

    def test_rollback_after_rollback_log_write
      @s.write_and_commit([ [ 'foo', :write, 'first' ],
                            [ 'bar', :write, 'second' ]
                          ])

      assert_exception(Higgs::Storage::DebugRollbackAfterRollbackLogWriteException) {
        @s.write_and_commit([ [ 'foo', :write, 'third' ],
                              [ 'foo', :update_properties, { :comment => 'Hello world.' } ],
                              [ 'bar', :delete ],
                              [ 'baz', :write, 'fourth' ],
                              [ 'nop', :__debug_rollback_after_rollback_log_write__ ]
                            ])
      }

      assert_equal('first', @s.fetch('foo'))
      assert_equal({}, @s.fetch_properties('foo')['custom_properties'])
      assert_equal('second', @s.fetch('bar'))
      assert_nil(@s.fetch('baz'))
    end

    def test_rollback_after_commit_log_write
      @s.write_and_commit([ [ 'foo', :write, 'first' ],
                            [ 'bar', :write, 'second' ]
                          ])

      assert_exception(Higgs::Storage::DebugRollbackAfterCommitLogWriteException) {
        @s.write_and_commit([ [ 'foo', :write, 'third' ],
                              [ 'foo', :update_properties, { :comment => 'Hello world.' } ],
                              [ 'bar', :delete ],
                              [ 'baz', :write, 'fourth' ],
                              [ 'nop', :__debug_rollback_after_commit_log_write__ ]
                            ])
      }

      assert_equal('first', @s.fetch('foo'))
      assert_equal({}, @s.fetch_properties('foo')['custom_properties'])
      assert_equal('second', @s.fetch('bar'))
      assert_nil(@s.fetch('baz'))
    end

    def test_rollback_commit_completed
      @s.write_and_commit([ [ 'foo', :write, 'first' ],
                            [ 'bar', :write, 'second' ]
                          ])

      assert_exception(Higgs::Storage::DebugRollbackCommitCompletedException) {
        @s.write_and_commit([ [ 'foo', :write, 'third' ],
                              [ 'foo', :update_properties, { :comment => 'Hello world.' } ],
                              [ 'bar', :delete ],
                              [ 'baz', :write, 'fourth' ],
                              [ 'nop', :__debug_rollback_commit_completed__ ]
                            ])
      }

      assert_equal('third', @s.fetch('foo'))
      assert_equal({ :comment => 'Hello world.' }, @s.fetch_properties('foo')['custom_properties'])
      assert_nil(@s.fetch('bar'))
      assert_equal('fourth', @s.fetch('baz'))
    end

    def test_rollback_log_erased
      @s.write_and_commit([ [ 'foo', :write, 'first' ],
                            [ 'bar', :write, 'second' ]
                          ])

      assert_exception(Higgs::Storage::DebugRollbackLogDeletedException) {
        @s.write_and_commit([ [ 'foo', :write, 'third' ],
                              [ 'foo', :update_properties, { :comment => 'Hello world.' } ],
                              [ 'bar', :delete ],
                              [ 'baz', :write, 'fourth' ],
                              [ 'nop', :__debug_rollback_log_deleted__ ]
                            ])
      }

      assert_equal('third', @s.fetch('foo'))
      assert_equal({ :comment => 'Hello world.' }, @s.fetch_properties('foo')['custom_properties'])
      assert_nil(@s.fetch('bar'))
      assert_equal('fourth', @s.fetch('baz'))
    end

    def test_key
      assert_equal(false, (@s.key? 'foo'))
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      assert_equal(true, (@s.key? 'foo'))
      @s.write_and_commit([ [ 'foo', :delete ] ])
      assert_equal(false, (@s.key? 'foo'))
    end

    def test_key_read_only
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.shutdown
      @s = Higgs::Storage.new(@name, :read_only => true)

      assert_equal(true, (@s.key? 'foo'))
      assert_equal(false, (@s.key? 'bar'))
    end

    def test_key_TypeError
      assert_exception(TypeError) { @s.key? :foo }
    end

    def test_each_key
      @s.each_key do |key|
        assert_fail('not exist any key')
      end

      @s.write_and_commit([ [ 'foo', :write, 'one' ],
                            [ 'bar', :write, 'two' ],
                            [ 'baz', :write, 'three' ]
                          ])

      expected_keys = %w[ foo bar baz ]
      @s.each_key do |key|
        assert((expected_keys.include? key), "each_key do |#{key}|")
        expected_keys.delete(key)
      end
      assert(expected_keys.empty?)

      @s.write_and_commit([ [ 'bar', :delete ] ])

      expected_keys = %w[ foo baz ]
      @s.each_key do |key|
        assert((expected_keys.include? key), "each_key do |#{key}|")
        expected_keys.delete(key)
      end
      assert(expected_keys.empty?)
    end

    def test_each_key_read_only
      @s.write_and_commit([ [ 'foo', :write, 'one' ],
                            [ 'bar', :write, 'two' ],
                            [ 'baz', :write, 'three' ]
                          ])
      @s.shutdown
      @s = Higgs::Storage.new(@name, :read_only => true)

      expected_keys = %w[ foo bar baz ]
      @s.each_key do |key|
        assert((expected_keys.include? key), "each_key do |#{key}|")
        expected_keys.delete(key)
      end
      assert(expected_keys.empty?)
    end

    def test_dump
      out = ''
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.write_and_commit([ [ 'foo', :write, "HALO" ] ]) # make gap
      @s.dump(out)
      assert(out.length > 0)
    end

    def test_verify
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.verify
    end

    def test_verify_BrokenError
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      File.open(@name + '.tar', File::WRONLY) {|w|
        size = w.stat.size
        w.seek(size - BLKSIZ * 5)
        w.write(0xFF.chr * BLKSIZ)
        w.fsync
      }
      assert_exception(Higgs::Storage::BrokenError) {
        @s.verify
      }
    end
  end
end
