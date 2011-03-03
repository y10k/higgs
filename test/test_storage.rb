#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'digest'
require 'fileutils'
require 'higgs/storage'
require 'logger'
require 'pp' if $DEBUG
require 'test/unit'

module Higgs::Test
  module StorageTestCase
    include Higgs

    def setup
      srand(0)                  # preset for rand
      @test_dir = 'st_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @st_name = File.join(@test_dir, 'foo')
      @logger = proc{|path|
        logger = Logger.new(path, 1)
        logger.level = Logger::DEBUG
        logger
      }
      @st = new_storage
    end

    def teardown
      @st.shutdown unless @st.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end
  end

  class StorageTest < Test::Unit::TestCase
    include StorageTestCase

    def new_storage
      Storage.new(@st_name, :logger => @logger)
    end

    def test_raw_write_and_commit
      write_list = [
        [ :write, :foo, :data, 'foo', 0xFF.chr * 1024 ],
        [ :write, :foo, :data, 'foo', "Hello world.\n" ],
        [ :write, :foo, :data, 'foo', "Hello world.\n" ],
        [ :write, :foo, :data, 'foo', 0xFF.chr * 1024 ],
        [ :write, :foo, :data, 'foo', "Hello world.\n" ],
        [ :delete, :foo ],
        [ :write, :foo, :data, 'foo', "Hello world.\n" ],
        [ :write, :foo, :data, 'foo', 0xFF.chr * 1024 ]
      ]
      @st.transaction{|tx|
        tx.raw_write_and_commit(write_list)
      }
    end

    def test_write_and_commit
      write_list = [
        [ :write, :foo, '' ],
        [ :delete, :foo ],
        [ :write, :foo, "Hello world.\n" ],
        [ :system_properties, :foo, { 'string_only' => true } ],
        [ :custom_properties, :foo, { 'TestDate' => '2007-04-29' } ]
      ]
      @st.transaction{|tx|
        tx.write_and_commit(write_list)
      }
    end

    def test_write_and_commit_fetch
      @st.transaction{|tx|
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))

        # add
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])

        assert_equal("Hello world.\n", tx.fetch('foo'))
        properties = tx.fetch_properties('foo')
        assert_equal(Digest::MD5.hexdigest("Hello world.\n"), properties['system_properties']['hash_value'])
        assert_equal(false, properties['system_properties']['string_only'])
        assert_equal({}, properties['custom_properties'])

        # update properties
        tx.write_and_commit([ [ :system_properties, 'foo', { 'string_only' => true } ] ])
        tx.write_and_commit([ [ :custom_properties, 'foo', { :comment => 'test' } ] ])

        assert_equal("Hello world.\n", tx.fetch('foo'))
        properties = tx.fetch_properties('foo')
        assert_equal(Digest::MD5.hexdigest("Hello world.\n"), properties['system_properties']['hash_value'])
        assert_equal(true, properties['system_properties']['string_only'])
        assert_equal({ :comment => 'test' }, properties['custom_properties'])

        # update
        tx.write_and_commit([ [ :write, 'foo', "Good bye.\n" ] ])

        assert_equal("Good bye.\n", tx.fetch('foo'))
        properties = tx.fetch_properties('foo')
        assert_equal(Digest::MD5.hexdigest("Good bye.\n"), properties['system_properties']['hash_value'])
        assert_equal(true, properties['system_properties']['string_only'])
        assert_equal({ :comment => 'test' }, properties['custom_properties'])

        # delete
        tx.write_and_commit([ [ :delete, 'foo' ] ])

        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))
      }
    end

    def test_write_and_commit_fetch_zero_bytes
      @st.transaction{|tx|
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))

        tx.write_and_commit([ [ :write, 'foo', '' ] ])

        assert_equal('', tx.fetch('foo'))
        properties = tx.fetch_properties('foo')
        assert_equal(Digest::MD5.hexdigest(''), properties['system_properties']['hash_value'])
        assert_equal({}, properties['custom_properties'])
      }
    end

    def test_write_and_commit_fetch_delete_no_data
      @st.transaction{|tx|
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))

        tx.write_and_commit([ [ :delete , 'foo'] ])

        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))
      }
    end

    def test_write_and_commit_read_only_NotWritableError
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => true, :logger => @logger)
      assert_raise(NoMethodError) {
        @st.transaction{|tx|
          tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
        }
      }
    end

    def test_write_and_commit_standby_NotWritableError
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => :standby, :logger => @logger)
      assert_raise(NoMethodError) {
        @st.transaction{|tx|
          tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
        }
      }
    end

    def test_write_and_commit_IndexError_not_exist_properties
      @st.transaction{|tx|
        assert_raise(IndexError) {
          tx.write_and_commit([ [ :system_properties, 'foo', {} ] ])
        }
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))

        assert_raise(IndexError) {
          tx.write_and_commit([ [ :custom_properties, 'foo', {} ] ])
        }
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))

        assert_raise(IndexError) {
          write_list = [
            [ :write, 'foo', "Hello world.\n" ],
            [ :delete, 'foo' ],
            [ :system_properties, 'foo', {} ]
          ]
          tx.write_and_commit(write_list)
        }
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))

        assert_raise(IndexError) {
          write_list = [
            [ :write, 'foo', "Hello world.\n" ],
            [ :delete, 'foo' ],
            [ :custom_properties, 'foo', {} ]
          ]
          tx.write_and_commit(write_list)
        }
        assert_nil(tx.fetch('foo'))
        assert_nil(tx.fetch_properties('foo'))
      }
    end

    def test_write_and_commit_TypeError_value_not_string
      assert_raise(TypeError) {
        @st.transaction{|tx|
          tx.write_and_commit([ [ :write, 'foo', "Hello world.\n".to_sym ] ])
        }
      }
    end

    def test_write_and_commit_ArgumentError_operation_unknown
      assert_raise(ArgumentError) {
        @st.transaction{|tx|
          tx.write_and_commit([ [ :unknown, 'foo', "Hello world.\n" ] ])
        }
      }
    end

    def test_system_properties
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, :foo, 'apple' ] ])

        cre_time = tx.fetch_properties(:foo)['system_properties']['created_time']
        chg_time = tx.fetch_properties(:foo)['system_properties']['changed_time']
        mod_time = tx.fetch_properties(:foo)['system_properties']['modified_time']

        sleep(0.001)
        tx.write_and_commit([ [ :write, :foo, 'banana' ] ])

        assert_equal(cre_time, tx.fetch_properties(:foo)['system_properties']['created_time'])
        assert_equal(chg_time, tx.fetch_properties(:foo)['system_properties']['changed_time'])
        assert(tx.fetch_properties(:foo)['system_properties']['modified_time'] > mod_time)

        mod_time2 = tx.fetch_properties(:foo)['system_properties']['modified_time']
        sleep(0.001)
        tx.write_and_commit([ [ :custom_properties, :foo, { 'bar' => 'orange' } ] ])

        assert_equal(cre_time, tx.fetch_properties(:foo)['system_properties']['created_time'])
        assert(tx.fetch_properties(:foo)['system_properties']['changed_time'] > chg_time)
        assert_equal(mod_time2, tx.fetch_properties(:foo)['system_properties']['modified_time'])

        chg_time2 = tx.fetch_properties(:foo)['system_properties']['changed_time']
        sleep(0.001)
        tx.write_and_commit([ [ :system_properties, :foo, { 'string_only' => true } ] ])

        assert_equal(cre_time, tx.fetch_properties(:foo)['system_properties']['created_time'])
        assert(tx.fetch_properties(:foo)['system_properties']['changed_time'] > chg_time2)
        assert_equal(mod_time2, tx.fetch_properties(:foo)['system_properties']['modified_time'])
      }
    end

    def test_change_number
      @st.transaction{|tx|
        assert_equal(nil, tx.data_change_number(:foo))
        assert_equal(nil, tx.properties_change_number(:foo))

        tx.write_and_commit([ [ :write, :foo, 'apple' ] ])
      }

      @st.transaction{|tx|
        assert_equal(1, tx.data_change_number(:foo))
        assert_equal(1, tx.properties_change_number(:foo))

        tx.write_and_commit([ [ :custom_properties, :foo, { 'bar' => 'banana' } ] ])
      }

      @st.transaction{|tx|
        assert_equal(1, tx.data_change_number(:foo))
        assert_equal(2, tx.properties_change_number(:foo))

        tx.write_and_commit([ [ :write, :foo, 'orange' ] ])
      }

      @st.transaction{|tx|
        assert_equal(3, tx.data_change_number(:foo))
        assert_equal(3, tx.properties_change_number(:foo))

        tx.write_and_commit([ [ :system_properties, :foo, { 'string_only' => true } ] ])
      }

      @st.transaction{|tx|
        assert_equal(3, tx.data_change_number(:foo))
        assert_equal(4, tx.properties_change_number(:foo))
      }
    end

    def test_key
      @st.transaction{|tx|
        assert_equal(false, (tx.key? 'foo'))
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
        assert_equal(true, (tx.key? 'foo'))
        tx.write_and_commit([ [ :delete , 'foo' ] ])
        assert_equal(false, (tx.key? 'foo'))
      }
    end

    def test_key_read_only
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => true, :logger => @logger)

      @st.transaction{|tx|
        assert_equal(true, (tx.key? 'foo'))
        assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_key_standby
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => :standby, :logger => @logger)

      @st.transaction{|tx|
        assert_equal(true, (tx.key? 'foo'))
        assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_each_key
      @st.transaction{|tx|
        tx.each_key do |key|
          flunk('not to reach')
        end

        tx.write_and_commit([ [ :write, 'foo', 'one' ],
                               [ :write, 'bar', 'two' ],
                               [ :write, 'baz', 'three' ]
                             ])

        expected_keys = %w[ foo bar baz ]
        tx.each_key do |key|
          assert(expected_keys.delete(key), "each_key do |#{key}|")
        end
        assert(expected_keys.empty?)

        tx.write_and_commit([ [ :delete, 'bar' ] ])

        expected_keys = %w[ foo baz ]
        tx.each_key do |key|
          assert(expected_keys.delete(key), "each_key do |#{key}|")
        end
        assert(expected_keys.empty?)
      }
    end

    def test_each_key_read_only
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', 'one' ],
                               [ :write, 'bar', 'two' ],
                               [ :write, 'baz', 'three' ]
                             ])
      }
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => true, :logger => @logger)

      @st.transaction{|tx|
        expected_keys = %w[ foo bar baz ]
        tx.each_key do |key|
          assert(expected_keys.delete(key), "each_key do |#{key}|")
        end
        assert(expected_keys.empty?)
      }
    end

    def test_each_key_standby
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', 'one' ],
                               [ :write, 'bar', 'two' ],
                               [ :write, 'baz', 'three' ]
                             ])
      }
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => :standby, :logger => @logger)

      @st.transaction{|tx|
        expected_keys = %w[ foo bar baz ]
        tx.each_key do |key|
          assert(expected_keys.delete(key), "each_key do |#{key}|")
        end
        assert(expected_keys.empty?)
      }
    end

    def test_verify
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
      @st.verify
    end

    def test_verify_BrokenError_mismatch_content_hash
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
      File.open(@st_name + '.tar', File::WRONLY) {|w|
        size = w.stat.size

        data_body_offset = size - Tar::Block::BLKSIZ * 5
        # EOA -> 2 blocks
        # properties body -> 1 block
        # properties head -> 1 block
        # data body -> 1 block
        # total 5 blocks skip from the end of file

        w.seek(data_body_offset)
        w.write(0xFF.chr * Tar::Block::BLKSIZ)
      }
      assert_raise(Storage::PanicError) {
        @st.verify
      }
    end

    def test_verify_BrokenError_failed_to_read_data
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
      File.open(@st_name + '.tar', File::WRONLY) {|w|
        size = w.stat.size

        data_head_offset = size - Tar::Block::BLKSIZ * 6
        # EOA -> 2 blocks
        # properties body -> 1 block
        # properties head -> 1 block
        # data body -> 1 block
        # data head -> 1 block
        # total 6 blocks skip from the end of file 

        w.truncate(data_head_offset)
        t = Tar::ArchiveWriter.new(w)
        t.write_EOA
        t.close(false)
      }
      assert_raise(Storage::PanicError) {
        @st.verify
      }
    end

    def test_verify_BrokenError_failed_to_read_properties
      @st.transaction{|tx|
        tx.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
      File.open(@st_name + '.tar', File::WRONLY) {|w|
        size = w.stat.size

        props_head_offset = size - Tar::Block::BLKSIZ * 4
        # EOA -> 2 blocks
        # properties body -> 1 block
        # properties head -> 1 block
        # total 4 blocks skip from the end of file 

        w.truncate(props_head_offset)
        t = Tar::ArchiveWriter.new(w)
        t.write_EOA
        t.close(false)
      }
      assert_raise(Storage::PanicError) {
        @st.verify
      }
    end

    def test_shutdown
      assert_equal(false, @st.shutdown?)
      assert_equal(true, @st.alive?)
      @st.shutdown
      assert_equal(true, @st.shutdown?)
      assert_equal(false, @st.alive?)
      assert_raise(Storage::ShutdownException) { @st.shutdown }
      assert_raise(Storage::ShutdownException) { @st.transaction{|tx| tx.fetch('foo') } }
      assert_raise(Storage::ShutdownException) { @st.transaction{|tx| tx.fetch_properties('foo') } }
      assert_raise(Storage::ShutdownException) { @st.transaction{|tx| tx.key? 'foo' } }
      assert_raise(Storage::ShutdownException) {
        @st.transaction{|tx|
          tx.each_key do
            flunk('not to reach')
          end
        }
      }
      assert_raise(Storage::ShutdownException) { @st.transaction{|tx| tx.write_and_commit([]) } }
      assert_raise(Storage::ShutdownException) { @st.verify }
    end

    def test_switch_to_write_RuntimeError_not_standby_mode
      assert_equal(false, @st.read_only)
      assert_raise(RuntimeError) {
        @st.switch_to_write
      }
    end

    def test_switch_to_write_RuntimeError_not_standby_mode_on_read_only_mode
      @st.shutdown
      @st = nil
      @st = Storage.new(@st_name, :read_only => true, :logger => @logger)

      assert_equal(true, @st.read_only)
      assert_raise(RuntimeError) {
        @st.switch_to_write
      }
    end
  end

  class StorageSwitchToWriteTest < StorageTest
    def new_storage
      st = Storage.new(@st_name, :logger => @logger, :read_only => :standby)
      assert_equal(:standby, st.read_only)
      st.switch_to_write
      assert_equal(false, st.read_only)
      st
    end

    for name in instance_methods(true)
      case (name)
      when /^test_.*read_only/, /^test_.*standby/
        undef_method name
      end
    end
  end

  class StorageRecoveryTest < Test::Unit::TestCase
    include StorageTestCase

    def dump_value(value)
      puts caller[0]
      pp value
    end
    private :dump_value

    def new_storage
      Storage.new(@st_name,
                  :jlog_rotate_max => 0, # unlimited rotation
                  :logger => @logger)
    end

    def write_data(loop_count=100, data_count=10, data_max_size=1024*5)
      loop_count.times do
        @st.transaction{|tx|
          write_list = []
          ope = [ :write, :delete ][rand(2)]
          key = rand(data_count)
          case (ope)
          when :write
            type = [ :a, :b ][rand(2)]
            value = rand(256).chr * rand(data_max_size)
            write_list << [ ope, key, type, key.to_s, value ]
          when :delete
            next unless (tx.key? key)
            write_list << [ ope, key ]
          else
            raise "unknown operation: #{ope}"
          end
          tx.raw_write_and_commit(write_list)
        }
      end
    end
    private :write_data

    def test_manual_recovery
      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      write_data

      3.times do
        @st.rotate_journal_log(false)
      end
      3.times do
        @st.rotate_journal_log(true)
      end

      @st.shutdown

      for name in Storage.rotated_entries("#{@st_name}.jlog")
        name =~ /\.jlog.*$/ or raise 'mismatch'
        FileUtils.cp(name, other_name + $&, :preserve => true)
      end
      Storage.recover(other_name)

      assert(FileUtils.cmp("#{@st_name}.tar", "#{other_name}.tar"), 'DATA should be same.')
      assert(Index.new.load("#{@st_name}.idx").to_h ==
             Index.new.load("#{other_name}.idx").to_h, 'INDEX should be same.')
    end

    def test_recovery_PanicError_unexpected_storage_id
      other_name = File.join(@test_dir, 'bar')
      st2 = Storage.new(other_name)
      st2.shutdown

      write_data

      3.times do
        @st.rotate_journal_log(false)
      end
      3.times do
        @st.rotate_journal_log(true)
      end

      @st.shutdown

      for name in Storage.rotated_entries("#{@st_name}.jlog")
        name =~ /\.jlog.*$/ or raise 'mismatch'
        FileUtils.cp(name, other_name + $&, :preserve => true)
      end

      assert_raise(Storage::PanicError) {
        Storage.recover(other_name)
      }
    end

    def test_auto_recovery
      write_data
      @st.rotate_journal_log(true)

      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      # write_data(10 * 10 * 256) < jlog_rotate_size(256 * 1024)
      write_data(10, 10, 256)

      # not closed journal log for other storage.
      FileUtils.cp("#{@st_name}.jlog", "#{other_name}.jlog", :preserve => true)

      @st.shutdown

      # auto-recovery for other storage
      st2 = Storage.new(other_name, :logger => @logger)
      st2.shutdown

      assert(FileUtils.cmp("#{@st_name}.tar", "#{other_name}.tar"), 'DATA should be same.')

      index1 = MVCCIndex.new.load("#{@st_name}.idx").to_h
      dump_value(index1) if $DEBUG
      index2 = MVCCIndex.new.load("#{other_name}.idx").to_h
      dump_value(index2) if $DEBUG
      assert(index1 == index2, 'INDEX should be same.')

      assert(FileUtils.cmp("#{@st_name}.jlog", "#{other_name}.jlog"), 'JOURNAL LOG should be same.')
    end

    def test_auto_recovery_on_standby_mode
      write_data
      @st.rotate_journal_log(true)

      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      # write_data(10 * 10 * 256) < jlog_rotate_size(256 * 1024)
      write_data(10, 10, 256)

      # not closed journal log for other storage.
      FileUtils.cp("#{@st_name}.jlog", "#{other_name}.jlog", :preserve => true)

      @st.shutdown

      # auto-recovery for other storage
      st2 = Storage.new(other_name, :logger => @logger, :read_only => :standby)
      st2.shutdown

      assert(FileUtils.cmp("#{@st_name}.tar", "#{other_name}.tar"), 'DATA should be same.')

      index1 = MVCCIndex.new.load("#{@st_name}.idx").to_h
      dump_value(index1) if $DEBUG
      index2 = MVCCIndex.new.load("#{other_name}.idx").to_h
      dump_value(index2) if $DEBUG

      assert(FileUtils.cmp("#{@st_name}.jlog", "#{other_name}.jlog"), 'JOURNAL LOG should be same.')
    end

    def test_auto_recovery_NotWritableError
      write_data
      @st.rotate_journal_log(true)

      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      # write_data(10 * 10 * 256) < jlog_rotate_size(256 * 1024)
      write_data(10, 10, 256)

      # not closed journal log for other storage.
      FileUtils.cp("#{@st_name}.jlog", "#{other_name}.jlog", :preserve => true)

      @st.shutdown

      # auto-recovery for other storage
      assert_raise(Higgs::Storage::NotWritableError) {
        st2 = Storage.new(other_name, :logger => @logger, :read_only => true)
        st2.shutdown
        flunk('not to reach')
      }
    end

    def test_recovery_PanicError_lost_journal_log_error
      write_data
      @st.rotate_journal_log(true)

      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      write_data
      @st.rotate_journal_log(true)
      write_data

      FileUtils.cp("#{@st_name}.jlog", "#{other_name}.jlog", :preserve => true)

      assert_raise(Storage::PanicError) {
        st2 = Storage.new(other_name, :logger => @logger)
      }
    end

    def test_apply_journal_log
      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      write_data
      @st.rotate_journal_log(true)
      @st.shutdown

      st2 = Storage.new(other_name, :jlog_rotate_size => 1024 * 8)
      begin
        for path in Storage.rotated_entries("#{@st_name}.jlog")
          st2.apply_journal_log(path)
        end
      ensure
        st2.shutdown
      end

      assert(FileUtils.cmp("#{@st_name}.tar", "#{other_name}.tar"), 'DATA should be same.')

      index1 = MVCCIndex.new.load("#{@st_name}.idx").to_h
      dump_value(index1) if $DEBUG
      index2 = MVCCIndex.new.load("#{other_name}.idx").to_h
      dump_value(index2) if $DEBUG
      assert(index1 == index2, 'INDEX should be same.')
    end

    def test_apply_journal_log_on_standby_mode
      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      write_data
      @st.rotate_journal_log(true)
      @st.shutdown

      st2 = Storage.new(other_name, :jlog_rotate_size => 1024 * 8, :read_only => :standby)
      begin
        for path in Storage.rotated_entries("#{@st_name}.jlog")
          st2.apply_journal_log(path)
        end
      ensure
        st2.shutdown
      end

      assert(FileUtils.cmp("#{@st_name}.tar", "#{other_name}.tar"), 'DATA should be same.')

      index1 = MVCCIndex.new.load("#{@st_name}.idx").to_h
      dump_value(index1) if $DEBUG
      index2 = MVCCIndex.new.load("#{other_name}.idx").to_h
      dump_value(index2) if $DEBUG
      assert(index1 == index2, 'INDEX should be same.')
    end

    def test_apply_journal_log_PanicError_unexpected_storage_id
      write_data
      @st.rotate_journal_log(true)
      @st.shutdown

      other_name = File.join(@test_dir, 'bar')
      st2 = Storage.new(other_name, :jlog_rotate_size => 1024 * 8)
      assert_equal(true, st2.alive?, 'alive storage')
      assert_equal(false, st2.shutdown?)

      begin
        for path in Storage.rotated_entries("#{@st_name}.jlog")
          assert_raise(Storage::PanicError) {
            st2.apply_journal_log(path)
          }
        end
        assert_equal(false, st2.alive?, 'panic storage')
        assert_equal(false, st2.shutdown?)
      ensure
        st2.shutdown
      end

      assert_equal(false, st2.alive?, 'shutdown storage')
      assert_equal(true, st2.shutdown?)
    end

    def test_apply_journal_log_PanicError_lost_journal_log
      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      write_data
      write_data
      write_data
      @st.rotate_journal_log(true)
      @st.shutdown

      st2 = Storage.new(other_name, :jlog_rotate_size => 1024 * 8)
      assert_equal(true, st2.alive?, 'alive storage')
      assert_equal(false, st2.shutdown?)

      begin
        entries = Storage.rotated_entries("#{@st_name}.jlog")
        entries.shift           # skip first journal log
        for path in entries
          assert_raise(Storage::PanicError) {
            st2.apply_journal_log(path)
          }
        end
        assert_equal(false, st2.alive?, 'panic storage')
        assert_equal(false, st2.shutdown?)
      ensure
        st2.shutdown
      end

      assert_equal(false, st2.alive?, 'shutdown storage')
      assert_equal(true, st2.shutdown?)
    end

    def test_apply_journal_log_from_online_backup
      write_data
      @st.rotate_journal_log(true)

      other_name = File.join(@test_dir, 'bar')
      FileUtils.cp("#{@st_name}.tar", "#{other_name}.tar", :preserve => true)
      FileUtils.cp("#{@st_name}.idx", "#{other_name}.idx", :preserve => true)

      other_name = File.join(@test_dir, 'bar')
      for name in Storage.rotated_entries("#{@st_name}.jlog")
        name =~ /\.jlog.*$/ or raise 'mismatch'
        FileUtils.cp(name, other_name + $&, :preserve => true)
        FileUtils.rm(name)
      end
      Storage.recover(other_name)

      write_data
      @st.rotate_journal_log(true)
      @st.shutdown

      other_name = File.join(@test_dir, 'bar')
      st2 = Storage.new(other_name, :jlog_rotate_size => 1024 * 8)
      begin
        for path in Storage.rotated_entries("#{@st_name}.jlog")
          st2.apply_journal_log(path)
        end
      ensure
        st2.shutdown
      end

      assert(FileUtils.cmp("#{@st_name}.tar", "#{other_name}.tar"), 'DATA should be same.')

      index1 = MVCCIndex.new.load("#{@st_name}.idx").to_h
      dump_value(index1) if $DEBUG
      index2 = MVCCIndex.new.load("#{other_name}.idx").to_h
      dump_value(index2) if $DEBUG
      assert(index1 == index2, 'INDEX should be same.')
    end
  end

  class ReadOnlyStorageFirstOpenErrorTest < Test::Unit::TestCase
    include Higgs

    def setup
      @test_dir = 'st_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @st_name = File.join(@test_dir, 'foo')
      @logger = proc{|path|
        logger = Logger.new(path, 1)
        logger.level = Logger::DEBUG
        logger
      }
    end

    def teardown
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def test_read_only_first_open_error
      begin
        Storage.new(@st_name, :read_only => true, :logger => @logger)
      rescue Errno::ENOENT
        return
      rescue java.io.FileNotFoundException
        return
      ensure
        error = $!
      end

      if (error) then
        flunk("unexpected error: #{error}")
      else
        flunk('no raise!')
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
