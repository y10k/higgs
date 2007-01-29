# $Id$

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

    class Reader
      include Block
      include Enumerable

      def initialize(input)
        @input = input
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
  end
end
