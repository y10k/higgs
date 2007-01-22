# $Id$

require 'tank'

module Tank
  class TarError < Error
  end

  class TarReadFailError < TarError
  end

  class TarFormatError < TarError
  end

  class TarMagicError < TarFormatError
  end

  class TarVersionError < TarFormatError
  end

  class TarCheckSumError < TarFormatError
  end

  class TarNonPortableFileTypeError < TarFormatError
  end

  class TarTooLongPathError < TarFormatError
  end

  module TarBlock
    # for ident(1)
    CVS_ID = '$Id$'

    # tar header format
    # -
    # name     : Z100 : null terminated string, primary hard link name
    # mode     : A8   : octet number format ascii string
    # uid      : A8   : octet number format ascii string
    # gid      : A8   : octet number format ascii string
    # size     : A12  : octet number format ascii string
    # mtime    : A12  : octet number format ascii string, seconds since epoch date-time (UTC 1970-01-01 00:00:00)
    # cksum    : A8   : octet number format ascii string
    # typeflag : a1   : ascii number character (null character is old tar format)
    # linkname : Z100 : null terminated string, secondly hard link name
    # magic    : A6   : white space terminated string
    # version  : a2   : 2 ascii characters
    # uname    : Z32  : null terminated string
    # gname    : Z32  : null terminated string
    # devmajor : Z8   : octet number format ascii string
    # devminor : Z8   : octet number format ascii string
    # prefix   : Z155 : null terminated string

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

    def read_header(skip_body=false)
      head_data = @input.read(BLKSIZ) or return
      if (head_data == EOA) then
	next_head_data = @input.read(BLKSIZ)
        if (next_head_data && next_head_data == EOA) then
          return nil
        else
          raise TarFormatError, "not of EOF: #{head_data.inspect}, #{next_head_data.inspect}"
        end
      end
      chksum = 0
      head_data[0...148].each_byte do |c|
	chksum += c
      end
      (' ' * 8).each_byte do |c|
	chksum += c
      end
      head_data[156...BLKSIZ].each_byte do |c|
	chksum += c
      end
      head_list = head_data.unpack(HEAD_FMT)
      head = {}
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
        head[k] = head_list[i]
      end
      if (head[:typeflag] == AREGTYPE) then
	head[:typeflag] = REGTYPE
      end
      if (head[:magic] != MAGIC) then
	raise TarMagicError, "unknown format: #{head[:magic].inspect}"
      end
      # why?
      #if (head[:version] != VERSION) then
      #  raise TarVersionError, "unknown version: #{head[:version].inspect}"
      #end
      [ :mode,
        :uid,
        :gid,
        :size,
        :mtime,
        :chksum
      ].each do |sym|
	head[sym] = head[sym].oct
      end
      if (head[:chksum] != chksum) then
	raise TarCheckSumError, 'broken tar'
      end
      head[:mtime] = Time.at(head[:mtime])
      if (skip_body) then
        skip_size = head[:size] + padding_size(head[:size])
        skip_blocks = skip_size / BLKSIZ
        while (skip_blocks > 0 && @input.read(BLKSIZ))
          skip_blocks -= 1
        end
      end
      head
    end

    def fetch
      head_with_body = read_header or return
      if (head_with_body[:size] > 0) then
        head_with_body[:data] = @input.read(head_with_body[:size])
        unless (head_with_body[:data]) then
          raise TarReadFailError, 'failed to read data'
        end
        if (head_with_body[:data].size != head_with_body[:size]) then
          raise TarReadFailError,
            "mismatch body length: expected #{head_with_body[:size]} but was #{head_with_body[:data].size}"
        end
        padding_size = padding_size(head_with_body[:size])
        @input.read(padding_size) if (padding_size > 0)
      else
        head_with_body[:data] = nil
      end
      head_with_body
    end

    def each(skip_body=false)
      if (skip_body) then
        while (head = read_header(true))
          yield(head)
        end
      else
        while (head_with_body = fetch)
          yield(head_with_body)
        end
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

    def validate_header(head)
      # check name
      unless (head[:name]) then
        raise TarFormatError, 'not defined name'
      end
      if (head[:name].length > 100) then
        raise TarTooLongPathError, "too long name: #{head[:name]}"
      end

      # check of mode, uid, gid, devmajor, devminor
      for key in [ :mode, :uid, :gid, :devmajor, :devminor ]
        unless (value = head[key]) then
          raise TarFormatError, "not defined #{key}"
        end
        unless ((0x00_0000..0xFF_FFFF).include? head[key]) then
          raise TarFormatError, "out of range at #{key}: #{head[:key]}"
        end
      end

      # check of size, mtime
      for key in [ :size, :mtime ]
        unless (head[key]) then
          raise "not defined #{key}"
        end
        unless ((0x0_0000_0000..0xF_FFFF_FFFF).include? head[key]) then
          raise "out of range at #{key}: #{head[key]}"
        end
      end

      # 

      nil
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
