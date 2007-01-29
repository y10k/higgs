#!/usr/local/bin/ruby

require 'fileutils'
require 'rubyunit'
require 'tank/tar'

module Tank::Test
  class TarBlockTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    def test_padding_size
      assert_equal(0,   Tank::Tar::Block.padding_size(0))

      assert_equal(511, Tank::Tar::Block.padding_size(1))
      assert_equal(510, Tank::Tar::Block.padding_size(2))
      assert_equal(509, Tank::Tar::Block.padding_size(3))

      assert_equal(3,   Tank::Tar::Block.padding_size(509))
      assert_equal(2,   Tank::Tar::Block.padding_size(510))
      assert_equal(1,   Tank::Tar::Block.padding_size(511))

      assert_equal(0,   Tank::Tar::Block.padding_size(512))

      assert_equal(511, Tank::Tar::Block.padding_size(513))
      assert_equal(510, Tank::Tar::Block.padding_size(514))
      assert_equal(509, Tank::Tar::Block.padding_size(515))
    end

    def test_tar?
      begin
        FileUtils.mkdir_p('foo')
        File.open('foo/bar', 'wb') {|w| w << "HALO\n" }
        File.open('baz', 'wb') {|w| w << "Hello world.\n" }
        system('tar cf foo.tar foo baz') # required unix tar command

        assert((File.exist? 'foo.tar'))
        assert_equal(true,  (Tank::Tar::Block.tar? 'foo.tar'))
        assert_equal(false, (Tank::Tar::Block.tar? 'foo'))
        assert_equal(false, (Tank::Tar::Block.tar? 'foo/bar'))
        assert_equal(false, (Tank::Tar::Block.tar? 'baz'))
      ensure
        FileUtils.rm_f %w[ foo.tar foo/bar baz ]
        FileUtils.rm_rf('foo')
      end
    end
  end

  class TarReaderTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Tank::Tar::Block

    def setup
      # tar
      FileUtils.mkdir_p('foo')
      File.open('foo/bar', 'wb') {|w| w << "HALO\n" }
      File.open('baz', 'wb') {|w| w << "Hello world.\n" }
      system('tar cf foo.tar foo baz') # required unix tar command

      # target
      @input = File.open('foo.tar', 'rb')
      @tar = Tank::Tar::Reader.new(@input)
    end

    def teardown
      @input.close unless @input.closed?
      #system('cp foo.tar foo_debug.tar') # debug
      FileUtils.rm_f %w[ foo.tar foo/bar baz ]
      FileUtils.rm_rf('foo')
    end

    def test_fetch
      entry = @tar.fetch
      assert('foo/' == entry[:name] || 'foo' == entry[:name])
      assert_equal(0,                      entry[:size])
      assert_equal(File.stat('foo').mtime, entry[:mtime])
      assert_equal(DIRTYPE,                entry[:typeflag])
      assert_equal(MAGIC,                  entry[:magic])
      assert_equal(nil,                    entry[:data])

      entry = @tar.fetch
      assert_equal('foo/bar',                  entry[:name])
      assert_equal(5,                          entry[:size])
      assert_equal(File.stat('foo/bar').mtime, entry[:mtime])
      assert_equal(REGTYPE,                    entry[:typeflag])
      assert_equal(MAGIC,                      entry[:magic])
      assert_equal("HALO\n",                   entry[:data])

      entry = @tar.fetch
      assert_equal('baz',                  entry[:name])
      assert_equal(13,                     entry[:size])
      assert_equal(File.stat('baz').mtime, entry[:mtime])
      assert_equal(REGTYPE,                entry[:typeflag])
      assert_equal(MAGIC,                  entry[:magic])
      assert_equal("Hello world.\n",       entry[:data])

      entry = @tar.fetch
      assert_equal(nil, entry)
    end

    def test_each
      count = 0
      @tar.each do |entry|
	case (count)
	when 0
	  assert('foo/' == entry[:name] || 'foo' == entry[:name])
	  assert_equal(0,                      entry[:size])
	  assert_equal(File.stat('foo').mtime, entry[:mtime])
	  assert_equal(DIRTYPE,                entry[:typeflag])
	  assert_equal(MAGIC,                  entry[:magic])
	  assert_equal(nil,                    entry[:data])
	when 1
	  assert_equal('foo/bar',                  entry[:name])
	  assert_equal(5,                          entry[:size])
	  assert_equal(File.stat('foo/bar').mtime, entry[:mtime])
	  assert_equal(REGTYPE,                    entry[:typeflag])
	  assert_equal(MAGIC,                      entry[:magic])
	  assert_equal("HALO\n",                   entry[:data])
	when 2
	  assert_equal('baz',                  entry[:name])
	  assert_equal(13,                     entry[:size])
	  assert_equal(File.stat('baz').mtime, entry[:mtime])
	  assert_equal(REGTYPE,                entry[:typeflag])
	  assert_equal(MAGIC,                  entry[:magic])
	  assert_equal("Hello world.\n",       entry[:data])
	else
	  raise "unknown data: #{entry.inspect}"
	end
	count += 1
      end
      assert_equal(3, count)
    end

    def test_close
      @tar.close
      assert(@input.closed?)
    end
  end

  class TarReaderSyscallTest < TarReaderTest
    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      super
      @tar.syscall = true
    end
  end
end
