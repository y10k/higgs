#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/storage'
require 'test/unit'

module Higgs::StorageTest
  # for ident(1)
  CVS_ID = '$Id$'

  class StorageTest < Test::Unit::TestCase
    include Higgs

    def setup
      srand(0)			# preset for rand 
      @test_dir = 'st_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @name = File.join(@test_dir, 'foo')
      @st = Storage.new(@name)
    end

    def teardown
      @st.shutdown unless @st.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def test_raw_write_and_commit
      write_list = [
        [ :write, :foo, :data, 0xFF.chr * 1024 ],
        [ :write, :foo, :data, "Hello world.\n" ],
        [ :write, :foo, :data, "Hello world.\n" ],
        [ :write, :foo, :data, 0xFF.chr * 1024 ],
        [ :write, :foo, :data, "Hello world.\n" ],
        [ :delete, :foo ],
        [ :write, :foo, :data, "Hello world.\n" ],
        [ :write, :foo, :data, 0xFF.chr * 1024 ]
      ]
      @st.raw_write_and_commit(write_list)
    end

    def test_recover
      @st.shutdown
      @st = Storage.new(@name, :jlog_rotate_max => 0) # unlimited rotation

      loop_count = 100
      data_count = 10

      loop_count.times do
	write_list = []
	ope = [ :write, :delete ][rand(2)]
	key = rand(data_count)
	case (ope)
	when :write
	  type = [ :a, :b ][rand(2)]
	  value = rand(256).chr * rand(5120)
	  write_list << [ ope, key, type, value ]
	when :delete
	  write_list << [ ope, key ]
	else
	  raise "unknown operation: #{ope}"
	end
	@st.raw_write_and_commit(write_list)
      end

      3.times do
	@st.rotate_journal_log(false)
      end
      3.times do
	@st.rotate_journal_log(true)
      end

      @st.shutdown

      other_name = File.join(@test_dir, 'bar')
      for name in Storage.rotate_entries("#{@name}.jlog")
	name =~ /\.jlog.*$/ or raise 'mismatch'
	FileUtils.cp(name, other_name + $&)
      end
      Storage.recover(other_name)

      assert(FileUtils.cmp("#{@name}.tar", "#{other_name}.tar"))
      assert(FileUtils.cmp("#{@name}.idx", "#{other_name}.idx"))
    end

    def test_write_and_commit
      write_list = [
	[ :write, :foo, '' ],
	[ :delete, :foo ],
	[ :write, :foo, "Hello world.\n" ],
	[ :update_properties, :foo, { 'TestDate' => '2007-04-29' } ]
      ]
      @st.write_and_commit(write_list)
    end

    def test_write_and_commit_fetch
      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))

      # add
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])

      assert_equal("Hello world.\n", @st.fetch('foo'))
      properties = @st.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Hello world.\n"), properties['system_properties']['hash_value'])
      assert_equal({}, properties['custom_properties'])

      # update properties
      @st.write_and_commit([ [ :update_properties, 'foo', { :comment => 'test' } ] ])

      assert_equal("Hello world.\n", @st.fetch('foo'))
      properties = @st.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Hello world.\n"), properties['system_properties']['hash_value'])
      assert_equal({ :comment => 'test' }, properties['custom_properties'])

      # update
      @st.write_and_commit([ [ :write, 'foo', "Good bye.\n" ] ])

      assert_equal("Good bye.\n", @st.fetch('foo'))
      properties = @st.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest("Good bye.\n"), properties['system_properties']['hash_value'])
      assert_equal({ :comment => 'test' }, properties['custom_properties'])

      # delete
      @st.write_and_commit([ [ :delete, 'foo' ] ])

      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))
    end

    def test_write_and_commit_fetch_zero_bytes
      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))

      @st.write_and_commit([ [ :write, 'foo', '' ] ])

      assert_equal('', @st.fetch('foo'))
      properties = @st.fetch_properties('foo')
      assert_equal(Digest::SHA512.hexdigest(''), properties['system_properties']['hash_value'])
      assert_equal({}, properties['custom_properties'])
    end

    def test_write_and_commit_fetch_delete_no_data
      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))

      @st.write_and_commit([ [ :delete , 'foo'] ])

      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))
    end

    def test_write_and_commit_read_only_NotWritableError
      @st.shutdown
      @st = nil
      @st = Storage.new(@name, :read_only => true)
      assert_raise(Storage::NotWritableError) {
        @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      }
    end

    def test_write_and_commit_IndexError_not_exist_properties
      assert_raise(IndexError) {
        @st.write_and_commit([ [ :update_properties, 'foo', {} ] ])
      }
      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))

      assert_raise(IndexError) {
	write_list = [
	  [ :write, 'foo', "Hello world.\n" ],
	  [ :delete, 'foo' ],
	  [ :update_properties, 'foo', {} ]
	]
        @st.write_and_commit(write_list)
      }
      assert_nil(@st.fetch('foo'))
      assert_nil(@st.fetch_properties('foo'))
    end

    def test_write_and_commit_TypeError_value_not_string
      assert_raise(TypeError) {
        @st.write_and_commit([ [ :write, 'foo', "Hello world.\n".to_sym ] ])
      }
    end

    def test_write_and_commit_ArgumentError_operation_unknown
      assert_raise(ArgumentError) {
        @st.write_and_commit([ [ :unknown, 'foo', "Hello world.\n" ] ])
      }
    end

    def test_key
      assert_equal(false, (@st.key? 'foo'))
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      assert_equal(true, (@st.key? 'foo'))
      @st.write_and_commit([ [ :delete , 'foo' ] ])
      assert_equal(false, (@st.key? 'foo'))
    end

    def test_key_read_only
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      @st.shutdown
      @st = nil
      @st = Storage.new(@name, :read_only => true)

      assert_equal(true, (@st.key? 'foo'))
      assert_equal(false, (@st.key? 'bar'))
    end

    def test_each_key
      @st.each_key do |key|
        flunk('not to reach')
      end

      @st.write_and_commit([ [ :write, 'foo', 'one' ],
			     [ :write, 'bar', 'two' ],
			     [ :write, 'baz', 'three' ]
			   ])

      expected_keys = %w[ foo bar baz ]
      @st.each_key do |key|
	assert(expected_keys.delete(key), "each_key do |#{key}|")
      end
      assert(expected_keys.empty?)

      @st.write_and_commit([ [ :delete, 'bar' ] ])

      expected_keys = %w[ foo baz ]
      @st.each_key do |key|
        assert(expected_keys.delete(key), "each_key do |#{key}|")
      end
      assert(expected_keys.empty?)
    end

    def test_each_key_read_only
      @st.write_and_commit([ [ :write, 'foo', 'one' ],
			     [ :write, 'bar', 'two' ],
			     [ :write, 'baz', 'three' ]
			   ])
      @st.shutdown
      @st = nil
      @st = Storage.new(@name, :read_only => true)

      expected_keys = %w[ foo bar baz ]
      @st.each_key do |key|
        assert(expected_keys.delete(key), "each_key do |#{key}|")
      end
      assert(expected_keys.empty?)
    end

    def test_verify
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      @st.verify
    end

    def test_verify_BrokenError_mismatch_content_hash
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      File.open(@name + '.tar', File::WRONLY) {|w|
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
      assert_raise(Storage::BrokenError) {
        @st.verify
      }
    end

    def test_verify_BrokenError_failed_to_read_data
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      File.open(@name + '.tar', File::WRONLY) {|w|
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
      assert_raise(Storage::BrokenError) {
        @st.verify
      }
    end

    def test_verify_BrokenError_failed_to_read_properties
      @st.write_and_commit([ [ :write, 'foo', "Hello world.\n" ] ])
      File.open(@name + '.tar', File::WRONLY) {|w|
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
      assert_raise(Storage::BrokenError) {
        @st.verify
      }
    end

    def test_shutdown
      @st.shutdown
      assert_raise(Storage::ShutdownException) { @st.shutdown }
      assert_raise(Storage::ShutdownException) { @st.fetch('foo') }
      assert_raise(Storage::ShutdownException) { @st.fetch_properties('foo') }
      assert_raise(Storage::ShutdownException) { @st.key? 'foo' }
      assert_raise(Storage::ShutdownException) {
	@st.each_key do
	  flunk('not to reach')
	end
      }
      assert_raise(Storage::ShutdownException) { @st.write_and_commit([]) }
      assert_raise(Storage::ShutdownException) { @st.verify }
    end
  end
end
