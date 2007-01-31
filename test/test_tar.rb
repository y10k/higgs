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

  class IOReadTest < RUNIT::TestCase
    def open_for_read(filename)
      File.open(filename, 'rb')
    end

    def setup
      File.open('foo.dat', 'wb') {|w| w << '0123456789' }
      @io = open_for_read('foo.dat')
    end

    def teardown
      @io.close
      FileUtils.rm_f('foo.dat')
    end

    def test_read
      assert_equal(0, @io.tell)
      assert_equal('0', @io.read(1))
      assert_equal(1, @io.pos)
      assert_equal('12', @io.read(2))
      assert_equal(3, @io.tell)
      assert_equal('345', @io.read(3))
      assert_equal(6, @io.pos)
      assert_equal('6789', @io.read(5))
      assert_equal(10, @io.tell)
      assert_equal(nil, @io.read(7))
      assert_equal(10, @io.tell)
    end

    def test_seek
      @io.seek(10)
      assert_equal(10, @io.tell)
      assert_equal(nil, @io.read(1))
      @io.seek(9)
      assert_equal(9, @io.pos)
      assert_equal('9', @io.read(1))
      @io.seek(8)
      assert_equal(8, @io.tell)
      assert_equal('8', @io.read(1))
      @io.seek(7)
      assert_equal(7, @io.pos)
      assert_equal('7', @io.read(1))
      @io.seek(6)
      assert_equal(6, @io.tell)
      assert_equal('6', @io.read(1))
      @io.seek(5)
      assert_equal(5, @io.pos)
      assert_equal('5', @io.read(1))
      @io.seek(4)
      assert_equal(4, @io.tell)
      assert_equal('4', @io.read(1))
      @io.seek(3)
      assert_equal(3, @io.pos)
      assert_equal('3', @io.read(1))
      @io.seek(2)
      assert_equal(2, @io.tell)
      assert_equal('2', @io.read(1))
      @io.seek(1)
      assert_equal(1, @io.pos)
      assert_equal('1', @io.read(1))
      @io.seek(0)
      assert_equal(0, @io.tell)
      assert_equal('0', @io.read(1))
    end
  end

  class IOSysreadTest < IOReadTest
    def open_for_read(filename)
      Tank::Tar::RawIO.new(File.open(filename, 'rb'))
    end
  end

  class TarReaderTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include Tank::Tar::Block

    def open_for_read(filename)
      File.open(filename, 'rb')
    end

    def setup
      # tar
      FileUtils.mkdir_p('foo')
      File.open('foo/bar', 'wb') {|w| w << "HALO\n" }
      File.open('baz', 'wb') {|w| w << "Hello world.\n" }
      system('tar cf foo.tar foo baz') # required unix tar command

      # target
      @input = open_for_read('foo.tar')
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

    def open_for_read(filename)
      Tank::Tar::RawIO.new(File.open(filename, 'rb'))
    end
  end
end
