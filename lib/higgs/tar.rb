# $Id$

require 'tank'

module Tank
  class TarError < Error
  end

  class TarFormatError < TarError
  end

  class TarBlockError < TarFormatError
  end

  class TarMagicError < TarBlockError
  end

  class TarVersionError < TarBlockError
  end

  class TarCheckSumError < TarBlockError
  end

  class TarNonPortableFileTypeError < TarBlockError
  end

  class TarTooLongPathError < TarBlockError
  end

  class TarDataError < TarFormatError
  end

  class TarReadFailError < TarDataError
  end

  module TarBlock
    # for ident(1)
    CVS_ID = '$Id$'

    # block size
    BLKSIZ = 512

    # unix tar format parameters
    MAGIC = 'ustar'
    VERSION = '00'
    REGTYPE = '0'
    AREGTYPE = "\0"
    LNKTYPE = '1'
    SYMTYPE = '2'
    CHRTYPE = '3'
    BLKTYPE = '4'
    DIRTYPE = '5'
    FIFOTYPE = '6'
    CONTTYPE = '7'

    # for pack/unpack
    HEAD_FMT = 'Z100 A8 A8 A8 A12 A12 A8 a1 Z100 A6 a2 Z32 Z32 Z8 Z8 Z155'

    # end of archive
    EOA = "\0" * BLKSIZ

    def padding_size(bytes)
      r = bytes % BLKSIZ
      (r > 0) ? BLKSIZ - r : 0
    end
    module_function :padding_size
  end

  class TarReader
    # for ident(1)
    CVS_ID = '$Id$'

    include TarBlock
    include Enumerable

    def initialize(input)
      @input = input
    end

    def self.tar?(path)
      if (File.file? path) then
	head = File.open(path, 'rb') {|input| input.read(BLKSIZ) }
	if (head.length == BLKSIZ) then
	  if (head.unpack(HEAD_FMT)[9] == MAGIC) then
	    return true
	  end
	end
      end
      false
    end

    def read_header(skip_data=false)
      head = @input.read(BLKSIZ) or return
      if (head == EOA) then
	head = @input.read(BLKSIZ)
	if (! head || head == EOA) then
	  return nil
	end
      end
      chksum = 0
      head[0...148].each_byte do |c|
	chksum += c
      end
      (' ' * 8).each_byte do |c|
	chksum += c
      end
      head[156...BLKSIZ].each_byte do |c|
	chksum += c
      end
      head_list = head.unpack(HEAD_FMT)
      entry = {}
      [ :name,
        :mode,
        :uid,
        :gid,
        :size,
        :mtime,
        :chksum,
        :typeflag,
        :linkname,
        :magic,
        :version,
        :uname,
        :gname,
        :devmajor,
        :devminor,
        :prefix
      ].each_with_index do |k, i|
        entry[k] = head_list[i]
      end
      if (entry[:typeflag] == AREGTYPE) then
	entry[:typeflag] = REGTYPE
      end
      if (entry[:magic] != MAGIC) then
	raise TarMagicError, "unknown format: #{entry[:magic].inspect}"
      end
      # why?
      #if (entry[:version] != VERSION) then
      #  raise TarVersionError, "unknown version: #{entry[:version].inspect}"
      #end
      [ :mode,
        :uid,
        :gid,
        :size,
        :mtime,
        :chksum
      ].each do |sym|
	entry[sym] = entry[sym].oct
      end
      if (entry[:chksum] != chksum) then
	raise TarCheckSumError, 'broken tar'
      end
      entry[:mtime] = Time.at(entry[:mtime])
      if (skip_data) then
        skip_size = entry[:size] + padding_size(entry[:size])
        skip_blocks = skip_size / BLKSIZ
        while (skip_blocks > 0 && @input.read(BLKSIZ))
          skip_blocks -= 1
        end
      end
      entry
    end

    def fetch
      entry = read_header or return
      if (entry[:size] > 0) then
        entry[:data] = @input.read(entry[:size])
        unless (entry[:data]) then
          raise TarReadFailError, 'failed to read data'
        end
        padding_size = padding_size(entry[:size])
        @input.read(padding_size) if (padding_size > 0)
      else
        entry[:data] = nil
      end
      entry
    end

    def each
      while (entry = self.fetch)
	yield(entry)
      end
      self
    end

    def close
      @input.close
      nil
    end
  end

  class TarWriter
    # for idnet(1)
    CVS_ID = '$Id$'

    include TarBlock

    def initialize(output)
      @output = output
    end

    def write_header(entry)
      if (entry.include? :ftype) then
        case (entry[:ftype])
        when 'file'
          entry[:typeflag] = REGTYPE
        when 'directory'
          entry[:typeflag] = DIRTYPE
        else
          raise TarNonPortableFileTypeError, "non-portable file type: #{entry[:ftype]}"
        end
      end
      if (entry.include? :typeflag) then
        case (entry[:typeflag])
        when REGTYPE, AREGTYPE, LNKTYPE, SYMTYPE, CHRTYPE, BLKTYPE, FIFOTYPE, CONTTYPE
          name = entry[:path]
        when DIRTYPE
          name = entry[:path] + '/'
        else
          raise "unknown typeflag: #{entry[:typeflag].inspect}"
        end
      else
        raise 'not defined typeflag'
      end
      if (name.length > 100) then
	raise TarTooLongPathError, "too long path: #{name}"
      end
      mode = format('%-8o', entry[:mode])
      uid = format('%-8o', entry[:uid])
      gid = format('%-8o', entry[:gid])
      size = format('%-12o', entry[:size])
      mtime = format('%-12o', entry[:mtime].to_i)
      chksum = ' ' * 8
      linkname = ''
      uname = entry[:uid].to_s
      gname = entry[:gid].to_s
      devmajor = ''
      devminor = ''
      prefix = ''
      head = [
	name, mode, uid, gid, size, mtime,
	chksum, entry[:typeflag], linkname, MAGIC, VERSION,
	uname, gname, devmajor, devminor, prefix
      ].pack(HEAD_FMT)
      head += "\0" * 12
      chksum2 = 0
      head.each_byte do |c|
	chksum2 += c
      end
      head[148, 8] = format('%-8o', chksum2)
      @output.write(head)
      nil
    end

    def add_file(path)
      stat = File.stat(path)
      entry = {
	:path => path,
	:ftype => stat.ftype,
	:mode => stat.mode,
	:uid => stat.uid,
	:gid => stat.gid,
	:size => (stat.file?) ? stat.size : 0,
	:mtime => stat.mtime
      }
      yield(entry) if block_given?
      write_header(entry)
      if (stat.ftype == 'file') then
	File.open(path, 'rb') {|input|
	  while (data = input.read(BLKSIZ))
	    @output.write(data)
	  end
	}
	padding_size = padding_size(entry[:size])
	@output.write("\0" * padding_size) if (padding_size > 0)
      end
      nil
    end

    def add_data(path, data, mtime=Time.now)
      entry = {
	:path => path,
	:ftype => 'file',
	:mode => 0100644,	# -rw-r--r--
	:uid => Process.euid,
	:gid => Process.egid,
	:size => data.length,
	:mtime => mtime
      }
      yield(entry) if block_given?
      write_header(entry)
      @output.write(data)
      padding_size = padding_size(entry[:size])
      @output.write("\0" * padding_size) if (padding_size > 0)
      nil
    end

    def close
      2.times do
	@output.write(EOA)
      end
      @output.flush
      @output.close
      nil
    end
  end
end
