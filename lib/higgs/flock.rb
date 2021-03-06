# -*- coding: utf-8 -*-

module Higgs
  class FileLock
    def initialize(path, read_only=false)
      @path = path
      @read_only = read_only
      rdwr_mode = (@read_only) ? File::RDONLY : File::RDWR
      begin
        @f = File.open(path, rdwr_mode | File::CREAT | File::EXCL, 0660)
      rescue Errno::EEXIST
        @f = File.open(path, rdwr_mode, 0660)
      end
      @f.binmode
      @f.set_encoding(Encoding::ASCII_8BIT)
    end

    attr_reader :read_only

    def read_lock
      @f.flock(File::LOCK_SH)
      nil
    end

    def write_lock
      if (@read_only) then
        raise 'read only'
      end
      @f.flock(File::LOCK_EX)
      nil
    end

    def unlock
      @f.flock(File::LOCK_UN)
      nil
    end

    def close
      unlock
      @f.close
      nil
    end

    def synchronize(mode=:EX)
      case (mode)
      when :SH
        read_lock
      when :EX
        write_lock
      else
        raise ArgumentError, "unknown lock mode: #{mode}"
      end

      begin
        r = yield
      ensure
        unlock
      end

      r
    end

    def self.open(*args)
      flock = new(*args)
      begin
        r = yield(flock)
      ensure
        flock.close
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
