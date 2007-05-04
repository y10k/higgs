# $Id$

require 'digest/sha2'
require 'higgs/cache'
require 'higgs/tar'
require 'higgs/thread'
require 'higgs2/index'
require 'higgs2/jlog'
require 'thread'
require 'yaml'

module Higgs
  class Storage
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(name)
      @name = name
      @tar_name = "#{@name}.tar"
      @idx_name = "#{@name}.idx"
      @jlog_name = "#{@name}.jlog"

      begin
	w_io = File.open(@tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
      rescue Errno::EEXIST
	w_io = File.open(@tar_name, File::WRONLY, 0660)
      end
      w_io.binmode
      @w_tar = Tar::ArchiveWriter.new(w_io)

      @number_of_read_io = 2
      @r_tar_pool = Thread::Pool.new(@number_of_read_io) {
	r_io = File.open(@tar_name, File::RDONLY)
	r_io.binmode
	Tar::ArchiveReader.new(Tar::RawIO.new(r_io))
      }

      @index = Index.new(@idx_name)
      @jlog = JournalLogger.open(@jlog_name)

      @commit_lock = Mutex.new
    end

    def shutdown
      @w_tar.fsync
      @index.close
      @jlog.close
      @r_tar_pool.shutdown{|r_tar|
	r_tar.close
      }
      @w_tar.close
      nil
    end

    def write_and_commit(write_list)
      @commit_lock.synchronize{
	commit_time = Time.now
	commit_completed = false

	c_num = @index.change_number.succ
	eoa = @index.eoa
	commit_log = {
	  :change_number => c_num,
	  :eoa => eoa,
	  :write_list => []
	}

	for key, ope, value in write_list
	  case (ope)
	  when :write
	    unless (value.kind_of? String) then
	      raise TypeError, "can't convert #{value.class} (value) to String"
	    end
	    unless (pos = Tar::Block.blocked_size(value.length)) then
	      
	    end
	  when :delete
	    
	  when :update_properties
	    
	  end
	end

	@jlog.write(commit_log)
      }
    end
  end
end
