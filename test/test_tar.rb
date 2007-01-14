#!/usr/local/bin/ruby

$: << File.join(File.dirname($0), '..', 'lib')

require 'fileutils'
require 'rubyunit'
require 'tank/tar'

module Tank::Test
  class TarReaderTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      # tar
      FileUtils.mkdir_p('foo')
      File.open('foo/bar', 'wb') {|output|
	output.print "HALO\n"
      }
      File.open('baz', 'wb') {|output|
	output.print "Hello world.\n"
      }
      system('tar cf foo.tar foo baz') # required unix tar command

      # target
      @input = File.open('foo.tar', 'rb')
      @tar = Tank::TarReader.new(@input)
    end

    def teardown
      @input.close unless @input.closed?
      #system('cp foo.tar foo_debug.tar') # debug
      FileUtils.rm_f %w( foo.tar foo/bar baz )
      FileUtils.rm_rf('foo')
    end

    def test_tar
      assert((File.exist? 'foo.tar'))
      assert_equal(true,  (Tank::TarReader.tar? 'foo.tar'))
      assert_equal(false, (Tank::TarReader.tar? 'foo'))
      assert_equal(false, (Tank::TarReader.tar? 'foo/bar'))
      assert_equal(false, (Tank::TarReader.tar? 'baz'))
    end

    def test_fetch
      entry = @tar.fetch
      assert('foo/' == entry[:name] || 'foo' == entry[:name])
      assert_equal(0,                       entry[:size])
      assert_equal(File.stat('foo').mtime,  entry[:mtime])
      assert_equal(Tank::TarBlock::DIRTYPE, entry[:typeflag])
      assert_equal(Tank::TarBlock::MAGIC,   entry[:magic])
      assert_equal(nil,                     entry[:data])

      entry = @tar.fetch
      assert_equal('foo/bar',                  entry[:name])
      assert_equal(5,                          entry[:size])
      assert_equal(File.stat('foo/bar').mtime, entry[:mtime])
      assert_equal(Tank::TarBlock::REGTYPE,    entry[:typeflag])
      assert_equal(Tank::TarBlock::MAGIC,      entry[:magic])
      assert_equal("HALO\n",                   entry[:data])

      entry = @tar.fetch
      assert_equal('baz',                   entry[:name])
      assert_equal(13,                      entry[:size])
      assert_equal(File.stat('baz').mtime,  entry[:mtime])
      assert_equal(Tank::TarBlock::REGTYPE, entry[:typeflag])
      assert_equal(Tank::TarBlock::MAGIC,   entry[:magic])
      assert_equal("Hello world.\n",        entry[:data])

      entry = @tar.fetch
      assert_equal(nil, entry)
    end

    def test_each
      count = 0
      @tar.each do |entry|
	case (count)
	when 0
	  assert('foo/' == entry[:name] || 'foo' == entry[:name])
	  assert_equal(0,                       entry[:size])
	  assert_equal(File.stat('foo').mtime,  entry[:mtime])
	  assert_equal(Tank::TarBlock::DIRTYPE, entry[:typeflag])
	  assert_equal(Tank::TarBlock::MAGIC,   entry[:magic])
	  assert_equal(nil,                     entry[:data])
	when 1
	  assert_equal('foo/bar',                  entry[:name])
	  assert_equal(5,                          entry[:size])
	  assert_equal(File.stat('foo/bar').mtime, entry[:mtime])
	  assert_equal(Tank::TarBlock::REGTYPE,    entry[:typeflag])
	  assert_equal(Tank::TarBlock::MAGIC,      entry[:magic])
	  assert_equal("HALO\n",                   entry[:data])
	when 2
	  assert_equal('baz',                   entry[:name])
	  assert_equal(13,                      entry[:size])
	  assert_equal(File.stat('baz').mtime,  entry[:mtime])
	  assert_equal(Tank::TarBlock::REGTYPE, entry[:typeflag])
	  assert_equal(Tank::TarBlock::MAGIC,   entry[:magic])
	  assert_equal("Hello world.\n",        entry[:data])
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

  class TarReaderTypeflagTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include File::Constants
    include Tank::TarBlock

    def setup
      @head = {
        :path => 'foo',
        :typeflag => nil,
        :mode => 0100644, # -rw-r--r--
        :uid => Process.euid,
        :gid => Process.egid,
        :size => 0,
        :mtime => Time.now
      }
    end

    def teardown
      FileUtils.rm_f('foo.tar')
    end

    def test_typeflag_REGTYPE
      @head[:typeflag] = REGTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', 'r') {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(REGTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_AREGTYPE
      @head[:typeflag] = AREGTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(REGTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_LNKTYPE
      @head[:typeflag] = LNKTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(LNKTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_SYMTYPE
      @head[:typeflag] = SYMTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(SYMTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_CHRTYPE
      @head[:typeflag] = CHRTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(CHRTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_BLKTYPE
      @head[:typeflag] = BLKTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(BLKTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_DIRTYPE
      @head[:typeflag] = DIRTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(DIRTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_FIFOTYPE
      @head[:typeflag] = FIFOTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(FIFOTYPE, tar.read_header[:typeflag])
      }
    end

    def test_typeflag_CONTTYPE
      @head[:typeflag] = CONTTYPE
      File.open('foo.tar', 'w') {|output|
        tar = Tank::TarWriter.new(output)
        tar.write_header(@head)
      }
      File.open('foo.tar', RDONLY) {|input|
        tar = Tank::TarReader.new(input)
        assert_equal(CONTTYPE, tar.read_header[:typeflag])
      }
    end
  end

  class TarWriterTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      # contents of tar
      FileUtils.mkdir_p('foo')
      File.open('foo/bar', 'wb') {|output|
	output.print "HALO\n"
      }

      # target
      @output = File.open('foo.tar', 'wb')
      @tar = Tank::TarWriter.new(@output)
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
      data_mtime = Time.now
      @tar.add_data('baz', "Hello world.\n", data_mtime)
      @tar.close
      assert(@output.closed?)
      File.open('foo.tar') {|input|
        tar = Tank::TarReader.new(input)
	count = 0
	for entry in tar
	  case (count)
	  when 0
	    assert_equal('foo/',                  entry[:name])
	    assert_equal(0,                       entry[:size])
	    assert_equal(File.stat('foo').mtime,  entry[:mtime])
	    assert_equal(Tank::TarBlock::DIRTYPE, entry[:typeflag])
	    assert_equal(Tank::TarBlock::MAGIC,   entry[:magic])
	    assert_equal(nil, entry[:data])
	  when 1
	    assert_equal('foo/bar',                  entry[:name])
	    assert_equal(5,                          entry[:size])
	    assert_equal(File.stat('foo/bar').mtime, entry[:mtime])
	    assert_equal(Tank::TarBlock::REGTYPE,    entry[:typeflag])
	    assert_equal(Tank::TarBlock::MAGIC,      entry[:magic])
	    assert_equal("HALO\n",                   entry[:data])
	  when 2
	    assert_equal('baz',                   entry[:name])
	    assert_equal(13,                      entry[:size])
	    assert_equal(data_mtime.to_i,         entry[:mtime].to_i)
	    assert_equal(Tank::TarBlock::REGTYPE, entry[:typeflag])
	    assert_equal(Tank::TarBlock::MAGIC,   entry[:magic])
	    assert_equal(Process.euid,            entry[:uid])
	    assert_equal(Process.egid,            entry[:gid])
	    assert_equal("Hello world.\n",        entry[:data])
	  else
	    raise "unknown data: #{entry.inspect}"
	  end
	  count += 1
	end
	assert_equal(3, count)
      }
    end
  end
end
