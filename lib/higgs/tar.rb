# $Id$

require 'forwardable'

module Tank
  module Tar
    # for ident(1)
    CVS_ID = '$Id$'

    class Error < StandardError
    end

    class FormatError < Error
    end

    class MagicError < FormatError
    end

    class VersionError < FormatError
    end

    class CheckSumError < FormatError
    end

    class TooLongPathError < FormatError
    end

    module Block
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

      def tar?(path)
        if (File.file? path) then
          head = File.open(path, 'rb') {|input| input.read(BLKSIZ) }
          if (head && head.length == BLKSIZ) then
            if (head.unpack(HEAD_FMT)[9] == MAGIC) then
              return true
            end
          end
        end
        false
      end
      module_function :tar?
    end

    class RawIO
      extend Forwardable

      def initialize(io)
        @io = io
      end

      def read(*args)
        begin
          @io.sysread(*args)
        rescue EOFError
          nil
        end
      end

      def write(*args)
        @io.syswrite(*args)
      end

      def seek(*args)
        @io.sysseek(*args)
      end

      def tell
        @io.sysseek(0, IO::SEEK_CUR)
      end

      alias pos tell

      def pos=(pos)
        @io.sysseek(pos, IO::SEEK_SET)
      end

      def_delegator :@io, :close
      def_delegator :@io, :closed?
    end

    class IOHandler
      extend Forwardable
      include Block

      def initialize(io)
        @io = io
      end

      def_delegator :@io, :seek
      def_delegator :@io, :tell
      def_delegator :@io, :pos
      def_delegator :@io, :pos=
      def_delegator :@io, :close
      def_delegator :@io, :closed?
    end

    class Reader < IOHandler
      include Enumerable

      def read_header(skip_body=false)
        head_data = @io.read(BLKSIZ) or raise FormatError, 'unexpected EOF'
        if (head_data == EOA) then
          next_head_data = @io.read(BLKSIZ)
          if (next_head_data && next_head_data == EOA) then
            return nil
          else
            raise FormatError, "not of EOA: #{head_data.inspect}, #{next_head_data.inspect}"
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
          raise MagicError, "unknown format: #{head[:magic].inspect}"
        end
        # why?
        #if (head[:version] != VERSION) then
        #  raise VersionError, "unknown version: #{head[:version].inspect}"
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
          raise CheckSumError, 'broken tar'
        end
        head[:mtime] = Time.at(head[:mtime])
        if (skip_body) then
          skip_size = head[:size] + padding_size(head[:size])
          @io.read(skip_size)
        end
        head
      end

      def fetch
        head_and_body = read_header or return
        if (head_and_body[:size] > 0) then
          blocked_size = head_and_body[:size] + padding_size(head_and_body[:size])
          head_and_body[:body] = @io.read(blocked_size) or raise FormatError, 'unexpected EOF'
          if (head_and_body[:body].size != blocked_size) then
            raise FormatError, 'mismatch body size'
          end
          head_and_body[:body][head_and_body[:size]...blocked_size] = ''
        else
          head_and_body[:body] = nil
        end
        head_and_body
      end

      def each(skip_body=false)
        if (skip_body) then
          while (head = read_header(true))
            yield(head)
          end
        else
          while (head_and_body = fetch)
            yield(head_and_body)
          end
        end
        self
      end
    end

    class Writer < IOHandler
      include Block

      def write_header(head)
        name = head[:name] or raise Error, "required name: #{head.inspect}"
        if (name.length > 100) then
          raise TooLongPathError, "too long path: #{head[:name]}"
        end
        mode = format('%-8o', head[:mode])
        uid = format('%-8o', head[:uid])
        gid = format('%-8o', head[:gid])
        size = format('%-12o', head[:size])
        mtime = format('%-12o', head[:mtime].to_i)
        dummy_chksum = ' ' * 8
        typeflag = head[:typeflag]
        linkname = head[:linkname] || ''
        magic = head[:magic] || MAGIC
        version = head[:version] || VERSION
        uname = head[:uname] || ''
        gname = head[:gname] || ''
        devmajor = head[:devmajor] || ''
        devminor = head[:devminor] || ''
        prefix = ''
        head = [
          name, mode, uid, gid, size, mtime,
          dummy_chksum, typeflag, linkname, magic, version,
          uname, gname, devmajor, devminor, prefix
        ].pack(HEAD_FMT)
        head += "\0" * 12
        chksum = 0
        head.each_byte do |c|
          chksum += c
        end
        head[148, 8] = format('%-8o', chksum)
        @io.write(head)
        nil
      end

      def write_EOA
        @io.write(EOA * 2)
        nil
      end

      FTYPE_TO_TAR = {
        'file' => REGTYPE,
        'directory' => DIRTYPE,
        'characterSpecial' => CHRTYPE,
        'blockSpecial' => BLKTYPE,
        'fifo' => FIFOTYPE,
        'link' => SYMTYPE,
        'socket' => FIFOTYPE
      }

      def add_file(path)
        stat = File.stat(path)
        unless (FTYPE_TO_TAR.include? stat.ftype) then
          raise Error, "unknown file type: #{stat.ftype}"
        end
        head = {
          :name => path,
          :mode => stat.mode,
          :uid => stat.uid,
          :gid => stat.gid,
          :size => (stat.file?) ? stat.size : 0,
          :mtime => stat.mtime,
          :typeflag => FTYPE_TO_TAR[stat.ftype]
        }
        yield(head) if block_given?
        write_header(head)
        if (stat.ftype == 'file') then
          File.open(path, 'rb') {|input|
            chunk_size = BLKSIZ * 128
            remaining_size = stat.size
            while (remaining_size > chunk_size)
              data = input.read(chunk_size) or raise Error, 'unexpected EOF'
              @io.write(data)
              remaining_size -= chunk_size
            end
            data = input.read(chunk_size) or raise Error, 'unexpected EOF'
            data += "\0" * padding_size(stat.size)
            @io.write(data)
          }
        end
        nil
      end

      def add_data(path, data, options=nil)
        head = {
          :name => path,
          :mode => 0100644,	# -rw-r--r--
          :uid => Process.euid,
          :gid => Process.egid,
          :size => data.length,
          :mtime => Time.now,
          :typeflag => REGTYPE
        }
        head.update(options) if options
        yield(head) if block_given?
        write_header(head)
        data += "\0" * padding_size(data.length)
        @io.write(data)
        nil
      end

      def close
        write_EOA
        super
      end
    end
  end
end
