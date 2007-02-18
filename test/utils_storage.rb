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

  module StorageTest
    include Higgs::Tar::Block

    def dbm_open
      raise NotImplementedError, 'not implemented'
    end

    def open_idx
      db = dbm_open[:write].call(@name + '.idx')
      begin
        yield(db)
      ensure
        db.close
      end
    end
    private :open_idx

    def new_storage(options={})
      options[:dbm_open] = dbm_open unless (options.include? :dbm_open)
      Higgs::Storage.new(@name, options)
    end
    private :new_storage

    def setup
      @tmp_dir = 'storage_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'storage_test')
      @s = new_storage
    end

    def teardown
      @s.shutdown if @s
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
      @s = nil
      @s = new_storage
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

    def test_write_and_commit_fetch_delete_no_data
      assert_nil(@s.fetch('foo'))
      assert_nil(@s.fetch_properties('foo'))

      @s.write_and_commit([ [ 'foo', :delete ] ])

      assert_nil(@s.fetch('foo'))
      assert_nil(@s.fetch_properties('foo'))
    end

    def test_write_and_commit_read_only_NotWritableError
      @s.shutdown
      @s = nil
      @s = new_storage(:read_only => true)
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

    def test_rollback_log_deleted
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

    def test_rollback_read_only_NotWritableError
      @s.shutdown
      @s = nil
      open_idx{|db|
        db['rollback'] = 'dummy_rollback_log'
      }
      assert_exception(Higgs::Storage::NotWritableError) {
        @s = new_storage(:read_only => true)
      }
    end

    def test_rollback_BrokenError_invalid_rollback_log
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert((db.key? 'EOA'))
        eoa = db['EOA'].to_i
        rollback_log = { :EOA => eoa, 'd:foo' => eoa }
        db['rollback'] = Marshal.dump(rollback_log)
      }
      assert_exception(Higgs::Storage::BrokenError) {
        @s = new_storage
      }
    end

    def test_rollback_BrokenError_shrinked_storage
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert((db.key? 'EOA'))
        eoa = db['EOA'].to_i
        rollback_log = { :EOA => eoa + 1 }
        db['rollback'] = Marshal.dump(rollback_log)
      }
      assert_exception(Higgs::Storage::BrokenError) {
        @s = new_storage
      }
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
      @s = nil
      @s = new_storage(:read_only => true)

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
      @s = nil
      @s = new_storage(:read_only => true)

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

    def test_verify_BrokenError_not_found_a_EOA
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert(db.delete('EOA'))
      }
      @s = new_storage
      assert_exception(Higgs::Storage::BrokenError) {
        @s.verify
      }
    end

    def test_verify_BrokenError_mismatch_content_hash
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

    def test_verify_BrokenError_failed_to_read_data
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert((db.key? 'EOA'))
        assert((db.key? 'd:foo'))
        eoa = db['EOA'].to_i
        pos = db['d:foo'].to_i
        assert(pos < eoa)
        db['d:foo'] = eoa.to_s
      }
      @s = new_storage
      assert_exception(Higgs::Storage::BrokenError) {
        @s.verify
      }
    end

    def test_verify_BrokenError_failed_to_read_properties
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert((db.key? 'EOA'))
        assert((db.key? 'p:foo'))
        eoa = db['EOA'].to_i
        pos = db['p:foo'].to_i
        assert(pos < eoa)
        db['p:foo'] = eoa.to_s
      }
      @s = new_storage
      assert_exception(Higgs::Storage::BrokenError) {
        @s.verify
      }
    end

    def test_verify_BrokenError_too_large_data_index
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert((db.key? 'EOA'))
        assert((db.key? 'd:foo'))
        eoa = db['EOA'].to_i
        pos = db['d:foo'].to_i
        assert(pos < eoa)
        db['d:foo'] = (eoa + BLKSIZ * 2).to_s
        File.open(@name + '.tar', 'a+') {|w|
          w.seek(pos)
          data = w.read(BLKSIZ * 2)
          w.write(data)
        }
      }
      @s = new_storage
      assert_exception(Higgs::Storage::BrokenError) {
        @s.verify
      }
    end

    def test_verify_BrokenError_too_large_properties_index
      @s.write_and_commit([ [ 'foo', :write, "Hello world.\n" ] ])
      @s.shutdown
      @s = nil
      open_idx{|db|
        assert((db.key? 'EOA'))
        assert((db.key? 'p:foo'))
        eoa = db['EOA'].to_i
        pos = db['p:foo'].to_i
        assert(pos < eoa)
        db['p:foo'] = (eoa + BLKSIZ * 2).to_s
        File.open(@name + '.tar', 'a+') {|w|
          w.seek(pos)
          data = w.read(BLKSIZ * 2)
          w.write(data)
        }
      }
      @s = new_storage
      assert_exception(Higgs::Storage::BrokenError) {
        @s.verify
      }
    end

    def test_reorganize
      # gap size == updated data size
      @s.write_and_commit([ [ 'foo', :write, 0xFF.chr * BLKSIZ * 1 ] ])
      @s.write_and_commit([ [ 'foo', :update_properties, { :comment => 'Hello world.' } ] ])

      @s.verify
      @s.reorganize
      @s.verify

      key_list = []
      @s.each_key do |key|
        key_list << key
      end
      assert_equal(%w[ foo ], key_list)

      # gap size > updated data size
      @s.write_and_commit([ [ 'foo', :write, 0xFF.chr * BLKSIZ * 0 ] ])
      @s.write_and_commit([ [ 'foo', :write, 0xFF.chr * BLKSIZ * 1 ] ])

      @s.verify
      @s.reorganize
      @s.verify

      key_list = []
      @s.each_key do |key|
        key_list << key
      end
      assert_equal(%w[ foo ], key_list)

      # gap size < updated data size
      @s.write_and_commit([ [ 'foo', :write, 0xFF.chr * 0 ] ])
      @s.write_and_commit([ [ 'foo', :write, 0xFF.chr * BLKSIZ * 100 ] ])

      @s.verify
      @s.reorganize
      @s.verify

      key_list = []
      @s.each_key do |key|
        key_list << key
      end
      assert_equal(%w[ foo ], key_list)
    end

    def test_reorganize_stress
      srand(0)
      num_tries = 3
      ope_count = 100
      commit_count = 10
      max_blks = 10

      num_tries.times do |nth|
        write_list = []
        key_list = []

        ope_count.times do |i|
          k = i.to_s
          key_list << k
          d = 0xFF.chr * BLKSIZ * rand(max_blks + 1)
          write_list << [ k, :write, d ]
          if (i % commit_count == 0) then
            @s.write_and_commit(write_list)
            write_list.clear
          end
        end
        unless (write_list.empty?) then
          @s.write_and_commit(write_list) 
          write_list.clear
        end

        @s.verify
        before_keys = []
        @s.each_key do |key|
          before_keys << key
        end
        before_keys.sort!

        @s.reorganize
        @s.verify
        after_keys = []
        @s.each_key do |key|
          after_keys << key
        end
        after_keys.sort!
        assert_equal(before_keys, after_keys, "nth: #{nth}")

        for i in 0..(key_list.length - 2)
          j = i + rand(key_list.length - i)
          key_list[i], key_list[j] = key_list[j], key_list[i]
        end

        ope_count.times do |i|
          k = key_list[i]

          # write : delete : update_properties = 2 : 1 : 1
          ope_dice = rand(4)
          case (ope_dice)
          when 0, 1
            d = 0xFF.chr * BLKSIZ * rand(max_blks + 1)
            write_list << [ k, :write, d ]
          when 2
            write_list << [ k, :delete ]
          when 3
            d = 'Z' * BLKSIZ * rand(max_blks + 1)
            write_list << [ k, :update_properties, { :memo => "#{nth}.#{i}", :padding => d } ]
          else
            raise "overflow ope_dice: #{ope_dice}"
          end

          if (i % commit_count == 0) then
            @s.write_and_commit(write_list)
            write_list.clear
          end
        end
        unless (write_list.empty?) then
          @s.write_and_commit(write_list)
          write_list.clear
        end

        @s.verify
        before_keys = []
        @s.each_key do |key|
          before_keys << key
        end
        before_keys.sort!

        @s.reorganize
        @s.verify
        after_keys = []
        @s.each_key do |key|
          after_keys << key
        end
        after_keys.sort!
        assert_equal(before_keys, after_keys, "nth: #{nth}")
      end
    end

    def test_reorganize_read_only_NotWritableError
      @s.shutdown
      @s = nil
      @s = new_storage(:read_only => true)
      assert_exception(Higgs::Storage::NotWritableError) {
        @s.reorganize
      }
    end
  end
end
