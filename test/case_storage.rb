#!/usr/local/bin/ruby

require 'digest/sha2'
require 'fileutils'
require 'higgs/cache'
require 'higgs/lock'
require 'higgs/storage'
require 'higgs/tar'
require 'rubyunit'
require 'yaml'

module Higgs::StorageTest
  # for ident(1)
  CVS_ID = '$Id$'

  module StorageTestCase
    include Higgs
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
      Storage.new(@name, options)
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
      assert_equal(Digest::SHA512.hexdigest(info_yml), properties['system_properties']['hash'])
      assert_instance_of(Time, properties['system_properties']['created_time'])
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
      assert_equal(Digest::SHA512.hexdigest("Hello world.\n"), properties['system_properties']['hash'])
      assert_equal({}, properties['custom_properties'])

      # update properties
      @s.write_and_commit([ [ 'foo', :update_properties, { :comment => 'test' } ] ])

      assert_equal("Hello world.\n", @s.fetch('foo'))
      properties = @s.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Hello world.\n"), properties['system_properties']['hash'])
      assert_equal({ :comment => 'test' }, properties['custom_properties'])

      # update
      @s.write_and_commit([ [ 'foo', :write, "Good bye.\n" ] ])

      assert_equal("Good bye.\n", @s.fetch('foo'))
      properties = @s.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Good bye.\n"), properties['system_properties']['hash'])
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
      assert_equal(Digest::SHA512.hexdigest(''), properties['system_properties']['hash'])
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
      assert_exception(Storage::NotWritableError) {
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

      assert_exception(Storage::DebugRollbackBeforeRollbackLogWriteException) {
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

      assert_exception(Storage::DebugRollbackAfterRollbackLogWriteException) {
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

      assert_exception(Storage::DebugRollbackAfterCommitLogWriteException) {
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

      assert_exception(Storage::DebugRollbackCommitCompletedException) {
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

      assert_exception(Storage::DebugRollbackLogDeletedException) {
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
      assert_exception(Storage::NotWritableError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      assert_exception(Storage::BrokenError) {
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
      @s.write_and_commit([ [ 'foo', :write, 0xFF.chr * BLKSIZ * 128 ] ])

      @s.verify
      @s.reorganize
      @s.verify

      key_list = []
      @s.each_key do |key|
        key_list << key
      end
      assert_equal(%w[ foo ], key_list)

      # gap stripes
      key_max = 1024
      write_list = []
      key_max.times do |i|
	write_list << [ i.to_s, :write, 0xFF.chr ]
      end
      @s.write_and_commit(write_list)
      del_list = []
      key_max.times do |i|
	if (i % 2 == 0) then
	  del_list << [ i.to_s, :delete ]
	end
      end
      @s.write_and_commit(del_list)

      @s.verify
      @s.reorganize
      @s.verify

      key_list = []
      @s.each_key do |key|
	key_list << key
      end
      expected_keys = %w[ foo ]
      expected_keys += (0...key_max).reject{|i| i % 2 == 0 }.map{|i| i.to_s }
      assert_equal(expected_keys.sort, key_list.sort)
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
          d = 0xFF.chr * BLKSIZ * rand(max_blks)
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
        before_alist = []
        @s.each_key do |key|
          before_alist << [ key, @s.fetch(key), @s.fetch_properties(key) ]
        end
        before_alist.sort!{|a, b| a[0] <=> b[0] }

        @s.reorganize
        @s.verify
        after_alist = []
        @s.each_key do |key|
          after_alist << [ key, @s.fetch(key), @s.fetch_properties(key) ]
        end
        after_alist.sort!{|a, b| a[0] <=> b[0] }
        assert_equal(before_alist, after_alist, "nth: #{nth}")

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
            d = 0xFF.chr * BLKSIZ * rand(max_blks)
            write_list << [ k, :write, d ]
          when 2
            write_list << [ k, :delete ]
          when 3
            d = 'Z' * BLKSIZ * rand(max_blks)
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
        before_alist = []
        @s.each_key do |key|
          before_alist << [ key, @s.fetch(key), @s.fetch_properties(key) ]
        end
        before_alist.sort!{|a, b| a[0] <=> b[0] }

        @s.reorganize
        @s.verify
        after_alist = []
        @s.each_key do |key|
          after_alist << [ key, @s.fetch(key), @s.fetch_properties(key) ]
        end
        after_alist.sort!{|a, b| a[0] <=> b[0] }
        assert_equal(before_alist, after_alist, "nth: #{nth}")
      end
    end

    def test_reorganize_read_only_NotWritableError
      @s.shutdown
      @s = nil
      @s = new_storage(:read_only => true)
      assert_exception(Storage::NotWritableError) {
        @s.reorganize
      }
    end

    def test_shutdown
      @s.shutdown
      s, @s = @s, nil
      assert_exception(Storage::ShutdownException) { s.shutdown }
      assert_exception(Storage::ShutdownException) { s.fetch('foo') }
      assert_exception(Storage::ShutdownException) { s.fetch_properties('foo') }
      assert_exception(Storage::ShutdownException) { s.key? 'foo' }
      assert_exception(Storage::ShutdownException) {
	s.each_key do
	  assert_fail('not to reach')
	end
      }
      assert_exception(Storage::ShutdownException) { s.write_and_commit([]) }
      assert_exception(Storage::ShutdownException) { s.reorganize }
      assert_exception(Storage::ShutdownException) { s.dump('') }
      assert_exception(Storage::ShutdownException) { s.verify }
    end
  end

  module StorageTransactionContextTestCase
    include Higgs

    def dbm_open
      raise NotImplementedError, 'not implemented'
    end

    def new_storage(options={})
      options[:dbm_open] = dbm_open unless (options.include? :dbm_open)
      Storage.new(@name, options)
    end
    private :new_storage

    def setup
      @tmp_dir = 'storage_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'storage_test')
      @s = new_storage
      @read_cache = Cache::SharedWorkCache.new{|key| @s.fetch(key) }
      @lock_manager = Lock::FineGrainLockManager.new
    end

    def teardown
      @s.shutdown if @s
      FileUtils.rm_rf(@tmp_dir)
    end

    def transaction
      r = nil
      @lock_manager.transaction{|lock_handler|
	tx = Storage::TransactionContext.new(@s, @read_cache, lock_handler)
	r = yield(tx)
	p tx.write_list if $DEBUG
	@s.write_and_commit(tx.write_list)
      }
      r
    end
    private :transaction

    def test_fetch
      @s.write_and_commit([ [ 'foo', :write, 'apple' ] ])
      transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
      }
    end

    def test_store
      transaction{|tx|
	tx['foo'] = 'apple'
      }
      assert_equal('apple', @s.fetch('foo'))
      assert_equal(nil,     @s.fetch('bar'))
    end

    def test_fetch_and_store
      transaction{|tx|
	assert_equal(nil, tx['foo'])
	assert_equal(nil, tx['bar'])

	tx['foo'] = 'apple'

	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
      }

      assert_equal('apple', @s.fetch('foo'))
      assert_equal(nil,     @s.fetch('bar'))

      transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
      }
    end

    def test_key
      @s.write_and_commit([ [ 'foo', :write, 'apple' ] ])
      transaction{|tx|
	assert_equal(true,  (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_fetch_and_key
      @s.write_and_commit([ [ 'foo', :write, 'apple' ] ])
      transaction{|tx|
	tx['foo']		# load to cache
	tx['bar']		# load to cache
	assert_equal(true, (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_key_and_store
      transaction{|tx|
	assert_equal(false, (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))

	tx['foo'] = 'apple'

	assert_equal(true,  (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }

      assert_equal(true,  (@s.key? 'foo'))
      assert_equal(false, (@s.key? 'bar'))

      transaction{|tx|
	assert_equal(true, (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_delete
      @s.write_and_commit([ [ 'foo', :write, 'apple' ],
			    [ 'bar', :write, 'banana' ]
			  ])
      transaction{|tx|
	assert_equal('apple', tx.delete('foo'))
	assert_equal(nil,     tx.delete('baz'))
      }
      assert_equal(nil,      @s.fetch('foo'))
      assert_equal('banana', @s.fetch('bar'))
      assert_equal(nil,      @s.fetch('baz'))
    end

    def test_store_and_delete
      transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
      }

      assert_equal('apple',  @s.fetch('foo'))
      assert_equal('banana', @s.fetch('bar'))

      transaction{|tx|
	assert_equal('apple', tx.delete('foo'))
      }

      assert_equal(nil,      @s.fetch('foo'))
      assert_equal('banana', @s.fetch('bar'))

      transaction{|tx|
	tx['foo'] = 'apple'
	assert_equal('apple', tx.delete('foo'))
      }

      assert_equal(nil,      @s.fetch('foo'))
      assert_equal('banana', @s.fetch('bar'))
    end

    def test_each_key
      @s.write_and_commit([ [ 'foo', :write, 'apple' ],
			    [ 'bar', :write, 'banana' ],
			    [ 'baz', :write, 'orange' ]
			  ])
      transaction{|tx|
	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert((expected_keys.include? key), "key: #{key}")
	  expected_keys.delete(key)
	end
	assert(expected_keys.empty?)
      }
    end

    def test_fetch_and_each_key
      @s.write_and_commit([ [ 'foo', :write, 'apple' ],
			    [ 'bar', :write, 'banana' ],
			    [ 'baz', :write, 'orange' ]
			  ])
      transaction{|tx|
	tx['alice']		# load to cache
	tx['bob']		# load to cache

	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert((expected_keys.include? key), "key: #{key}")
	  expected_keys.delete(key)
	end
	assert(expected_keys.empty?)
      }
    end

    def test_store_and_delete_and_each_key
      transaction{|tx|
	tx.each_key do |key|
	  assert_fail('not to reach')
	end

	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)

	tx.delete('bar')

	expected_keys = %w[ foo baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }

      expected_keys = %w[ foo baz ]
      @s.each_key do |key|
	assert_equal(expected_keys.delete(key), key)
      end
      assert(expected_keys.empty?)

      transaction{|tx|
	expected_keys = %w[ foo baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }
    end

    def test_store_and_fetch_and_key_zero_bytes
      transaction{|tx|
	tx['foo'] = ''

	assert_equal('',  tx['foo'])
	assert_equal(nil, tx['bar'])

	assert_equal(true,  (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }

      transaction{|tx|
	assert_equal('',  tx['foo'])
	assert_equal(nil, tx['bar'])

	assert_equal(true,  (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_store_and_each_key_zero_bytes
      transaction{|tx|
	tx.each_key do |key|
	  assert_fail('not to reach')
	end

	tx['foo'] = ''
	tx['bar'] = ''
	tx['baz'] = ''

	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }

      transaction{|tx|
	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }
    end

    def test_lock_and_unlock_and_locked
      transaction{|tx|
	assert_equal(false, (tx.locked? 'foo'))
	assert_equal(false, (tx.locked? 'bar'))

	tx.lock('foo')

	assert_equal(true, (tx.locked? 'foo'))
	assert_equal(false, (tx.locked? 'bar'))

	tx.unlock('foo')

	assert_equal(false, (tx.locked? 'foo'))
	assert_equal(false, (tx.locked? 'bar'))
      }
    end

    def test_lock_lock_lock_unlock
      transaction{|tx|
	tx.lock('foo')
	tx.lock('foo')
	tx.lock('foo')
	assert_equal(true, (tx.locked? 'foo'))
	tx.unlock('foo')
	assert_equal(false, (tx.locked? 'foo'))
      }
    end

    def test_auto_lock
      transaction{|tx|
	assert_equal(false, (tx.locked? 'foo'))
	tx['foo']
	assert_equal(true, (tx.locked? 'foo'))

	assert_equal(false, (tx.locked? 'bar'))
	tx['bar'] = 'banana'
	assert_equal(true, (tx.locked? 'bar'))

	assert_equal(false, (tx.locked? 'baz'))
	tx.delete('baz')
	assert_equal(true, (tx.locked? 'baz'))

	assert_equal(false, (tx.locked? 'qux'))
	tx.key? 'qux'
	assert_equal(true, (tx.locked? 'qux'))

	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
      }

      transaction{|tx|
	assert_equal(false, (tx.locked? 'foo'))
	assert_equal(false, (tx.locked? 'bar'))
	assert_equal(false, (tx.locked? 'baz'))
	assert_equal(false, (tx.locked? 'qux'))

	tx.each_key do |key|
	  # nothing to do.
	end

	assert_equal(true, (tx.locked? 'foo'))
	assert_equal(true, (tx.locked? 'bar'))
	assert_equal(true, (tx.locked? 'baz'))
	assert_equal(false, (tx.locked? 'qux'))
      }
    end

    def test_write_clear
      @s.write_and_commit([ [ 'foo', :write, 'apple' ] ])
      transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])

	tx.delete('foo')
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	assert_equal(nil,      tx['foo'])
	assert_equal('banana', tx['bar'])
	assert_equal('orange', tx['baz'])

	tx.write_clear

	assert_equal('apple',  tx['foo']) # cancel to delete
	assert_equal('banana', tx['bar']) # local cache reserved
	assert_equal('orange', tx['baz']) # local cache reserved
      }

      assert_equal('apple', @s.fetch('foo'))
      assert_equal(nil,     @s.fetch('bar'))
      assert_equal(nil,     @s.fetch('baz'))

      transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }
    end

    def test_rollback
      @s.write_and_commit([ [ 'foo', :write, 'apple' ] ])
      transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])

	tx.delete('foo')
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	assert_equal(nil,      tx['foo'])
	assert_equal('banana', tx['bar'])
	assert_equal('orange', tx['baz'])

	tx.rollback

	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }

      assert_equal('apple', @s.fetch('foo'))
      assert_equal(nil,     @s.fetch('bar'))
      assert_equal(nil,     @s.fetch('baz'))

      transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }
    end
  end
end
