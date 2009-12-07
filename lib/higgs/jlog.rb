# -*- coding: utf-8 -*-
# = journal log writer
# == license
#   :include:../../LICENSE
#

require 'higgs/block'

module Higgs
  # = journal log writer
  class JournalLogger
    include Block

    MAGIC_SYMBOL = 'HIGGS_JLOG'
    EOF_MARK = :END_OF_JLOG
    BIN_EOF_MARK = Marshal.dump(EOF_MARK)

    # see Higgs::Block#block_write for <tt>hash_type</tt>
    def initialize(out, sync=false, hash_type=:MD5)
      @out = out
      @sync = sync
      @hash_type = hash_type
    end

    def sync?
      @sync
    end

    def size
      @out.stat.size
    end

    def write(log, hash_type=nil)
      bin_log = Marshal.dump(log)
      start_pos = @out.tell
      commit_completed = false
      begin
        block_write(@out, MAGIC_SYMBOL, bin_log, hash_type || @hash_type)
        if (@sync) then
          @out.fsync
        else
          @out.flush
        end
        commit_completed = true
      ensure
        @out.truncate(start_pos) unless commit_completed
      end

      self
    end

    def write_EOF
      JournalLogger.eof_mark(@out)
      self
    end

    def close(eof=true)
      write_EOF if eof
      @out.fsync
      @out.close
      @out = nil
      nil
    end

    class << self
      def has_eof_mark?(path)
        File.open(path, 'r') {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          fsiz = f.stat.size
          if (fsiz < Block::BLOCK_SIZE * 2) then
            return false
          end
          f.seek(fsiz - Block::BLOCK_SIZE * 2)

          begin
            bin_log = Block.block_read(f, MAGIC_SYMBOL) or return
            log = Marshal.load(bin_log)
            if (log == EOF_MARK) then
              return true
            end
          rescue Block::BrokenError
            return false
          end
        }

        false
      end

      def need_for_recovery?(path)
        (File.exist? path) && ! (has_eof_mark? path)
      end

      def open(path, *args)
        begin
          f = File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0660)
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
        rescue Errno::EEXIST
          if (need_for_recovery? path) then
            raise "need for recovery: #{path}"
          end
          f = File.open(path, File::WRONLY, 0660)
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          fsiz = f.stat.size
          f.truncate(fsiz - Block::BLOCK_SIZE * 2)
        end
        f.seek(0, IO::SEEK_END)
        new(f, *args)
      end

      def eof_mark(out)
        Block.block_write(out, MAGIC_SYMBOL, BIN_EOF_MARK)
        nil
      end

      def scan_log(io)
        safe_pos = io.tell
        begin
          while (bin_log = Block.block_read(io, MAGIC_SYMBOL))
            log = Marshal.load(bin_log)
            if (log == EOF_MARK) then
              break
            end
            yield(log)
            safe_pos = io.tell
          end
        rescue Block::BrokenError
          io.seek(safe_pos)
          raise
        end
        self
      end

      def each_log(path)
        File.open(path, 'r') {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          scan_log(f) do |log|
            yield(log)
          end
        }
        nil
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
