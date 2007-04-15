# $Id$

require 'digest/sha2'
require 'higgs/cache'
require 'higgs/tar'
require 'higgs/thread'
require 'higgs2/index'
require 'higgs2/jlog'
require 'yaml'

module Higgs
  class Storage
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(name)
      @name = name
      @tar_name = "{@name}.tar"
      @idx_name = "{@name}.idx"
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
  end
end
