#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/tar'
require 'rubyunit'

module Higgs::TarTest
  # for ident(1)
  CVS_ID = '$Id$'

  class TarBlockTest < RUNIT::TestCase
    include Higgs::Tar

    def test_padding_size
      assert_equal(0,   Block.padding_size(0))

      assert_equal(511, Block.padding_size(1))
      assert_equal(510, Block.padding_size(2))
      assert_equal(509, Block.padding_size(3))

      assert_equal(3,   Block.padding_size(509))
      assert_equal(2,   Block.padding_size(510))
      assert_equal(1,   Block.padding_size(511))

      assert_equal(0,   Block.padding_size(512))

      assert_equal(511, Block.padding_size(513))
      assert_equal(510, Block.padding_size(514))
      assert_equal(509, Block.padding_size(515))
    end

    def test_tar?
      begin
        FileUtils.mkdir_p('foo')
        File.open('foo/bar', 'wb') {|w| w << "HALO\n" }
        File.open('baz', 'wb') {|w| w << "Hello world.\n" }
        system('tar cf foo.tar foo baz') # required unix tar command

        assert((File.exist? 'foo.tar'))
        assert_equal(true,  (Block.tar? 'foo.tar'))
        assert_equal(false, (Block.tar? 'foo'))
        assert_equal(false, (Block.tar? 'foo/bar'))
        assert_equal(false, (Block.tar? 'baz'))
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
      @io.pos = 9
      assert_equal(9, @io.pos)
      assert_equal('9', @io.read(1))
      @io.seek(8)
      assert_equal(8, @io.tell)
      assert_equal('8', @io.read(1))
      @io.pos = 7
      assert_equal(7, @io.pos)
      assert_equal('7', @io.read(1))
      @io.seek(6)
      assert_equal(6, @io.tell)
      assert_equal('6', @io.read(1))
      @io.pos = 5
      assert_equal(5, @io.pos)
      assert_equal('5', @io.read(1))
      @io.seek(4)
      assert_equal(4, @io.tell)
      assert_equal('4', @io.read(1))
      @io.pos = 3
      assert_equal(3, @io.pos)
      assert_equal('3', @io.read(1))
      @io.seek(2)
      assert_equal(2, @io.tell)
      assert_equal('2', @io.read(1))
      @io.pos = 1
      assert_equal(1, @io.pos)
      assert_equal('1', @io.read(1))
      @io.seek(0)
      assert_equal(0, @io.tell)
      assert_equal('0', @io.read(1))
    end
  end

  class IOSysreadTest < IOReadTest
    include Higgs::Tar

    def open_for_read(filename)
      RawIO.new(File.open(filename, 'rb'))
    end
  end

  class ReaderTest < RUNIT::TestCase
    include Higgs::Tar
    include Higgs::Tar::Block

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
      @tar = Reader.new(@input)
    end

    def teardown
      @input.close unless @input.closed?
      #system('cp foo.tar foo_debug.tar') # debug
      FileUtils.rm_f %w[ foo.tar foo/bar baz ]
      FileUtils.rm_rf('foo')
    end

    def test_fetch
      head_and_body = @tar.fetch
      assert('foo/' == head_and_body[:name] || 'foo' == head_and_body[:name])
      assert_equal(0,                      head_and_body[:size])
      assert_equal(File.stat('foo').mtime, head_and_body[:mtime])
      assert_equal(DIRTYPE,                head_and_body[:typeflag])
      assert_equal(MAGIC,                  head_and_body[:magic])
      assert_equal(nil,                    head_and_body[:body])

      head_and_body = @tar.fetch
      assert_equal('foo/bar',                  head_and_body[:name])
      assert_equal(5,                          head_and_body[:size])
      assert_equal(File.stat('foo/bar').mtime, head_and_body[:mtime])
      assert_equal(REGTYPE,                    head_and_body[:typeflag])
      assert_equal(MAGIC,                      head_and_body[:magic])
      assert_equal("HALO\n",                   head_and_body[:body])

      head_and_body = @tar.fetch
      assert_equal('baz',                  head_and_body[:name])
      assert_equal(13,                     head_and_body[:size])
      assert_equal(File.stat('baz').mtime, head_and_body[:mtime])
      assert_equal(REGTYPE,                head_and_body[:typeflag])
      assert_equal(MAGIC,                  head_and_body[:magic])
      assert_equal("Hello world.\n",       head_and_body[:body])

      head_and_body = @tar.fetch
      assert_equal(nil, head_and_body)
    end

    def test_each
      count = 0
      @tar.each do |head_and_body|
	case (count)
	when 0
	  assert('foo/' == head_and_body[:name] || 'foo' == head_and_body[:name])
	  assert_equal(0,                      head_and_body[:size])
	  assert_equal(File.stat('foo').mtime, head_and_body[:mtime])
	  assert_equal(DIRTYPE,                head_and_body[:typeflag])
	  assert_equal(MAGIC,                  head_and_body[:magic])
	  assert_equal(nil,                    head_and_body[:body])
	when 1
	  assert_equal('foo/bar',                  head_and_body[:name])
	  assert_equal(5,                          head_and_body[:size])
	  assert_equal(File.stat('foo/bar').mtime, head_and_body[:mtime])
	  assert_equal(REGTYPE,                    head_and_body[:typeflag])
	  assert_equal(MAGIC,                      head_and_body[:magic])
	  assert_equal("HALO\n",                   head_and_body[:body])
	when 2
	  assert_equal('baz',                  head_and_body[:name])
	  assert_equal(13,                     head_and_body[:size])
	  assert_equal(File.stat('baz').mtime, head_and_body[:mtime])
	  assert_equal(REGTYPE,                head_and_body[:typeflag])
	  assert_equal(MAGIC,                  head_and_body[:magic])
	  assert_equal("Hello world.\n",       head_and_body[:body])
	else
	  raise "unknown data: #{head_and_body.inspect}"
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

  class ReaderSyscallTest < ReaderTest
    def open_for_read(filename)
      RawIO.new(File.open(filename, 'rb'))
    end
  end

  class WriterTest < RUNIT::TestCase
    include Higgs::Tar
    include Higgs::Tar::Block

    def open_for_write(filename)
      File.open(filename, 'wb')
    end

    def setup
      # contents of tar
      FileUtils.mkdir_p('foo')
      File.open('foo/bar', 'wb') {|w| w << "HALO\n" }

      # target
      @output = open_for_write('foo.tar')
      @tar = Writer.new(@output)
    end

    def teardown
      @output.close unless @output.closed?
      #system('cp foo.tar foo_debug.tar') # debug
      FileUtils.rm_f %w(foo.tar foo/bar)
      FileUtils.rm_rf('foo')
    end

    def test_add
      @tar.add_file('foo')
      @tar.add_file('foo/bar')
      timestamp = Time.now
      @tar.add('baz', "Hello world.\n", :mtime => timestamp)
      @tar.close
      assert(@output.closed?)
      File.open('foo.tar') {|r|
        tar = Reader.new(r)
	count = 0
	for head_and_body in tar
	  case (count)
	  when 0
	    assert_equal('foo',                  head_and_body[:name])
	    assert_equal(0,                      head_and_body[:size])
	    assert_equal(File.stat('foo').mtime, head_and_body[:mtime])
	    assert_equal(DIRTYPE,                head_and_body[:typeflag])
	    assert_equal(MAGIC,                  head_and_body[:magic])
	    assert_equal(nil,                    head_and_body[:body])
	  when 1
	    assert_equal('foo/bar',                  head_and_body[:name])
	    assert_equal(5,                          head_and_body[:size])
	    assert_equal(File.stat('foo/bar').mtime, head_and_body[:mtime])
	    assert_equal(REGTYPE,                    head_and_body[:typeflag])
	    assert_equal(MAGIC,                      head_and_body[:magic])
	    assert_equal("HALO\n",                   head_and_body[:body])
	  when 2
	    assert_equal('baz',            head_and_body[:name])
	    assert_equal(13,               head_and_body[:size])
	    assert_equal(timestamp.to_i,   head_and_body[:mtime].to_i)
	    assert_equal(REGTYPE,          head_and_body[:typeflag])
	    assert_equal(MAGIC,            head_and_body[:magic])
	    assert_equal(Process.euid,     head_and_body[:uid])
	    assert_equal(Process.egid,     head_and_body[:gid])
	    assert_equal("Hello world.\n", head_and_body[:body])
	  else
	    raise "unknown data: #{head_and_body.inspect}"
	  end
	  count += 1
	end
	assert_equal(3, count)
      }
    end
  end

  class WriterSyscallTest < WriterTest
    def open_for_write(filename)
      RawIO.new(File.open(filename, 'wb'))
    end
  end

  class HeaderTest < RUNIT::TestCase
    include Higgs::Tar
    include Higgs::Tar::Block

    def setup
      @output = File.open('foo.tar', 'wb')
      @input = File.open('foo.tar', 'rb')
      @writer = Writer.new(@output)
      @reader = Reader.new(@input)
    end

    def teardown
      @output.close unless @output.closed?
      @input.close unless @input.closed?
      FileUtils.rm_f('foo.tar')
    end

    def test_read_header_FormatError_unexpected_EOF
      assert_equal(0, @input.stat.size)
      assert_exception(FormatError) { @reader.read_header }
    end

    def test_read_header_FormatError_too_short_header
      @output.write("foo\n")
      @output.flush
      assert_equal(4, @input.stat.size)
      assert_exception(FormatError) { @reader.read_header }
    end

    def test_read_header_FormatError_not_of_EOA
      @output.write("\0" * BLKSIZ)
      @output.flush
      assert_equal(BLKSIZ, @input.stat.size)
      assert_exception(FormatError) { @reader.read_header }
    end

    def test_read_header_typeflag_AREGTYPE
      @writer.add('foo', "Hello world.\n", :typeflag => AREGTYPE)
      @output.flush
      assert_equal(BLKSIZ * 2, @input.stat.size)
      head = @reader.read_header
      assert_equal(BLKSIZ, @input.pos)
      assert_equal('foo', head[:name])
      assert_equal(REGTYPE, head[:typeflag], 'AREGTYPE -> REGTYPE')
    end

    def test_read_header_MagicError_unknown_format
      @writer.add('foo', "Hello world.\n", :magic => 'unknown')
      @output.flush
      assert_equal(BLKSIZ * 2, @input.stat.size)
      assert_exception(MagicError) { @reader.read_header }
      assert_equal(BLKSIZ, @input.pos)
    end

    def test_read_header_CheckSumError_broken_tar
      @writer.add('foo', "Hello world.\n", :chksum => 0)
      @output.flush
      assert_equal(BLKSIZ * 2, @input.stat.size)
      assert_exception(CheckSumError) { @reader.read_header }
      assert_equal(BLKSIZ, @input.pos)
    end

    def test_read_header_skip_body
      @writer.add('foo', "Hello world.\n")
      @output.flush
      assert_equal(BLKSIZ * 2, @input.stat.size)
      @reader.read_header(true)
      assert_equal(BLKSIZ * 2, @input.pos)
    end
  end
end
