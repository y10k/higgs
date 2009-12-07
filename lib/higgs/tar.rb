# -*- coding: utf-8 -*-

require 'forwardable'
require 'higgs/exceptions'

module Higgs
  # = unix TAR utilities
  module Tar
    include Exceptions

    class Error < HiggsError
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

    class IOError < Error
    end

    # tar header format
    # 
    #  name     : Z100 : null terminated string, primary hard link name
    #  mode     : A8   : octet number format ascii string
    #  uid      : A8   : octet number format ascii string
    #  gid      : A8   : octet number format ascii string
    #  size     : A12  : octet number format ascii string
    #  mtime    : A12  : octet number format ascii string, seconds since epoch date-time (UTC 1970-01-01 00:00:00)
    #  cksum    : A8   : octet number format ascii string
    #  typeflag : a1   : ascii number character (null character is old tar format)
    #  linkname : Z100 : null terminated string, secondly hard link name
    #  magic    : A6   : white space terminated string
    #  version  : a2   : 2 ascii characters
    #  uname    : Z32  : null terminated string
    #  gname    : Z32  : null terminated string
    #  devmajor : Z8   : octet number format ascii string
    #  devminor : Z8   : octet number format ascii string
    #  prefix   : Z155 : null terminated string
    #
    module Block
      # block size
      BLKSIZ = 512

      # limit name length
      MAX_LEN = 100

      # unix tar format parameters
      MAGIC    = 'ustar'
      VERSION  = '00'
      REGTYPE  = '0'
      AREGTYPE = "\0"
      LNKTYPE  = '1'
      SYMTYPE  = '2'
      CHRTYPE  = '3'
      BLKTYPE  = '4'
      DIRTYPE  = '5'
      FIFOTYPE = '6'
      CONTTYPE = '7'            # reserved

      # for pack/unpack
      HEAD_FMT = 'Z100 A8 A8 A8 A12 A12 A8 a1 Z100 A6 a2 Z32 Z32 Z8 Z8 Z155'

      # end of archive
      EOA = "\0" * BLKSIZ

      def padding_size(bytes)
        r = bytes % BLKSIZ
        (r > 0) ? BLKSIZ - r : 0
      end
      module_function :padding_size

      def blocked_size(bytes)
        bytes + padding_size(bytes)
      end
      module_function :blocked_size

      def tar?(path)
        if (File.file? path) then
          head = File.open(path, 'r') {|r_io|
            r_io.binmode
            r_io.set_encoding(Encoding::ASCII_8BIT)
            r_io.read(BLKSIZ)
          }
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

      def self.open(path, *args)
        io = File.open(path, *args)
        io.binmode
        io.set_encoding(Encoding::ASCII_8BIT)
        RawIO.new(io)
      end

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

      def_delegator :@io, :flush
      def_delegator :@io, :fsync
      def_delegator :@io, :truncate
      def_delegator :@io, :close
      def_delegator :@io, :closed?
    end

    class IOHandler
      extend Forwardable
      include Block

      def initialize(io)
        @io = io
      end

      def to_io
        @io
      end

      def_delegator :@io, :seek
      def_delegator :@io, :tell
      def_delegator :@io, :pos
      def_delegator :@io, :pos=
      def_delegator :@io, :flush
      def_delegator :@io, :fsync
      def_delegator :@io, :truncate
      def_delegator :@io, :close
      def_delegator :@io, :closed?
    end

    class ArchiveReader < IOHandler
      include Enumerable

      def read_header(skip_body=false)
        head_data = @io.read(BLKSIZ)
        unless (head_data) then
          raise FormatError, 'unexpected EOF'
        end
        if (head_data.length != BLKSIZ) then
          raise FormatError, 'too short header'
        end
        if (head_data == EOA) then
          next_head_data = @io.read(BLKSIZ)
          if (next_head_data && next_head_data == EOA) then
            return nil
          else
            raise FormatError, "not of EOA: #{head_data.inspect}, #{next_head_data.inspect}"
          end
        end
        chksum = (head_data[0...148] + ' ' * 8 + head_data[156...BLKSIZ]).sum(20)
        head_list = head_data.unpack(HEAD_FMT)
        head = {}
        [ :name, :mode, :uid, :gid, :size, :mtime,
          :chksum, :typeflag, :linkname, :magic, :version,
          :uname, :gname, :devmajor, :devminor, :prefix
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
          blocked_size = blocked_size(head_and_body[:size])
          head_and_body[:body] = @io.read(blocked_size)
          unless (head_and_body) then
            raise FormatError, 'unexpected EOF'
          end
          if (head_and_body[:body].size != blocked_size) then
            raise FormatError, 'mismatch body size'
          end
          head_and_body[:body][head_and_body[:size]...blocked_size] = ''
        else
          case (head_and_body[:typeflag])
          when REGTYPE, CONTTYPE
            head_and_body[:body] = ''
          else
            head_and_body[:body] = nil
          end
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

    class ArchiveWriter < IOHandler
      include Block

      def write_header(head)
        name = head[:name] or raise FormatError, "required name: #{head.inspect}"
        if (name.length > MAX_LEN) then
          raise TooLongPathError, "too long path: #{name}"
        end
        mode     = format('%-8o', head[:mode] || 0100644) # 0100644 => -rw-r--r--
        uid      = format('%-8o', head[:uid] || Process.euid)
        gid      = format('%-8o', head[:gid] || Process.egid)
        size     = format('%-12o', head[:size])
        mtime    = format('%-12o', (head[:mtime] || Time.now).to_i)
        dummy_chksum = ' ' * 8
        typeflag = head[:typeflag] || REGTYPE
        linkname = head[:linkname] || ''
        magic    = head[:magic] || MAGIC
        version  = head[:version] || VERSION
        uname    = head[:uname] || ''
        gname    = head[:gname] || ''
        devmajor = head[:devmajor] || ''
        devminor = head[:devminor] || ''
        prefix   = ''
        header = [
          name, mode, uid, gid, size, mtime,
          dummy_chksum, typeflag, linkname, magic, version,
          uname, gname, devmajor, devminor, prefix
        ].pack(HEAD_FMT)
        header += "\0" * 12
        if (head.key? :chksum) then
          chksum = head[:chksum]
        else
          chksum = header.sum(20)
        end
        header[148, 8] = format('%-8o', chksum)
        @io.write(header)
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

      def add(name, body, options=nil)
        head = {
          :name => name,
          :size => body.length
        }
        if (options) then
          head.update(options) 
        end
        if (block_given?) then
          yield(head)
        end
        write_header(head)
        body += "\0" * padding_size(body.length)
        @io.write(body)
        nil
      end

      def add_file(path)
        stat = File.stat(path)
        unless (FTYPE_TO_TAR.key? stat.ftype) then
          raise FormatError, "unknown file type: #{stat.ftype}"
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
        if (block_given?) then
          yield(head)
        end
        write_header(head)
        if (stat.ftype == 'file') then
          File.open(path, 'r') {|r_io|
            r_io.binmode
            r_io.set_encoding(Encoding::ASCII_8BIT)
            chunk_size = BLKSIZ * 128
            remaining_size = stat.size
            while (remaining_size > chunk_size)
              s = r_io.read(chunk_size) or raise IOError, 'unexpected EOF'
              @io.write(s)
              remaining_size -= chunk_size
            end
            s = r_io.read(chunk_size) or raise IOError, 'unexpected EOF'
            s += "\0" * padding_size(stat.size)
            @io.write(s)
          }
        end
        nil
      end

      def close(eoa=true)
        write_EOA if eoa
        super()
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
