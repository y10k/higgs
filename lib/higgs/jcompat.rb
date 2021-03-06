# -*- coding: utf-8 -*-

require 'higgs/tar'
require 'java'

# for digest
require 'digest/md5'
#require 'digest/rmd160'
require 'digest/sha1'
require 'digest/sha2'

module Higgs
  module Tar
    # JRuby compatibility support
    class JRawIO
      def make_java_byte_array(size)
        ([ 0 ]  * size).to_java(:byte)
      end
      private :make_java_byte_array

      def initialize(path, mode='r')
        read_only = true
        case (mode)
        when String
          if (mode =~ /w/) then
            read_only = false
          end
        when Integer
          if ((mode & (File::WRONLY | File::RDWR)) != 0) then
            read_only = false
          end
        end
        @io = java.io.RandomAccessFile.new(path, read_only ? 'r' : 'rw')
        @buf = make_java_byte_array(1024)
        @closed = false
      end

      def read(size)
        @buf = make_java_byte_array(size) if (@buf.length < size)
        read_size = @io.read(@buf, 0, size)
        if (read_size > 0) then
          String.from_java_bytes(@buf)[0, read_size]
        elsif (read_size == 0) then
          ""
        else
          nil
        end
      end

      def write(str)
        @io.write(str.to_java_bytes)
        nil
      end

      def tell
        @io.getFilePointer
      end

      def seek(pos)
        @io.seek(pos)
        nil
      end

      alias pos tell
      alias pos= seek

      def flush
      end

      def fsync
        @io.getFD.sync
        nil
      end

      def truncate(size)
        @io.setLength(size)
        nil
      end

      def close
        @io.close
        @closed = true
        nil
      end

      def closed?
        @closed
      end
    end

    def RawIO.open(path, *args)
      JRawIO.new(path, *args)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
