# -*- coding: utf-8 -*-

require 'digest'
require 'forwardable'
require 'higgs/block'
require 'higgs/cache'
require 'higgs/flock'
require 'higgs/freeze'
require 'higgs/index'
require 'higgs/jlog'
require 'higgs/tar'
require 'higgs/thread'
require 'thread'
require 'yaml'

module Higgs
  # = transactional storage core
  class Storage
    include Exceptions

    class Error < HiggsError
    end

    class PanicError < Error
    end

    class NotWritableError < Error
    end

    class ShutdownException < Exceptions::ShutdownException
    end

    PROPERTIES_CKSUM_TYPE = 'SUM16'
    PROPERTIES_CKSUM_BITS = 16

    DATA_HASH = {}
    DATA_HASH_BIN = {}

    [ [ :SUM16,  proc{|s| s.sum(16).to_s              } ],
      [ :MD5,    proc{|s| Digest::MD5.hexdigest(s)    } ],
      [ :RMD160, proc{|s| Digest::RMD160.hexdigest(s) } ],
      [ :SHA1,   proc{|s| Digest::SHA1.hexdigest(s)   } ],
      [ :SHA256, proc{|s| Digest::SHA256.hexdigest(s) } ],
      [ :SHA384, proc{|s| Digest::SHA384.hexdigest(s) } ],
      [ :SHA512, proc{|s| Digest::SHA512.hexdigest(s) } ]
    ].each do |hash_symbol, hash_proc|
      DATA_HASH[hash_symbol] = hash_proc
      DATA_HASH_BIN[hash_symbol.to_s] = hash_proc
    end

    # options for Higgs::Storage
    module InitOptions
      # these options are defined.
      # [<tt>:number_of_read_io</tt>] number of read I/O handle of pool. default is <tt>2</tt>.
      # [<tt>:read_only</tt>] if <tt>true</tt> then storage is read-only. default is <tt>false</tt>.
      #                       <tt>:standby</tt> is standby mode. in standby mode, storage is read-only
      #                       and Higgs::Storage#apply_journal_log is callable.
      #                       if Higgs::Storage#switch_to_write is called in standby mode then
      #                       state of storage changes from standby mode to read-write mode.
      # [<tt>:properties_cache</tt>] read-cache for properties. default is a new instance of Higgs::LRUCache.
      # [<tt>:data_cache</tt>] read-cache for data. default is a new instance of Higgs::LRUCache.
      # [<tt>:data_hash_type</tt>] hash type (<tt>:SUM16</tt>, <tt>:MD5</tt>, <tt>:RMD160</tt>,
      #                            <tt>:SHA1</tt>, <tt>:SHA256</tt>, <tt>:SHA384</tt> or <tt>:SHA512</tt>)
      #                            for data check. default is <tt>:MD5</tt>.
      # [<tt>:jlog_sync</tt>] see Higgs::JournalLogger for detail. default is <tt>false</tt>.
      # [<tt>:jlog_hash_type</tt>] see Higgs::JournalLogger for detail. default is <tt>:MD5</tt>.
      # [<tt>:jlog_rotate_size</tt>] when this size is exceeded, journal log is switched to a new file.
      #                              default is <tt>1024 * 256</tt>.
      # [<tt>:jlog_rotate_max</tt>] old journal log is preserved in this number.
      #                             if <tt>:jlog_rotate_max</tt> is <tt>0</tt>, old journal log is
      #                             not deleted. if online-backup is used, <tt>:jlog_rotate_max</tt>
      #                             should be <tt>0</tt>. default is <tt>1</tt>.
      # [<tt>:logger</tt>] procedure to create a logger. default is a procedure to create a new
      #                    instance of Logger with logging level <tt>Logger::WARN</tt>.
      def init_options(options)
        @number_of_read_io = options[:number_of_read_io] || 2

        if (options.key? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        if (options.key? :properties_cache) then
          @properties_cache = options[:properties_cache]
        else
          @properties_cache = LRUCache.new
        end

        if (options.key? :data_cache) then
          @data_cache = options[:data_cache]
        else
          @data_cache = LRUCache.new
        end

        @data_hash_type = options[:data_hash_type] || :MD5
        unless (DATA_HASH.key? @data_hash_type) then
          raise ArgumentError, "unknown data hash type: #{@data_hash_type}"
        end

        if (options.key? :jlog_sync) then
          @jlog_sync = options[:jlog_sync]
        else
          @jlog_sync = false
        end

        @jlog_hash_type = options[:jlog_hash_type] || :MD5
        unless (Block::BODY_HASH.key? @jlog_hash_type) then
          raise ArgumentError, "unknown journal log hash type: #{@jlog_hash_type}"
        end

        @jlog_rotate_size = options[:jlog_rotate_size] || 1024 * 256
        @jlog_rotate_max = options[:jlog_rotate_max] || 1

        if (options.key? :logger) then
          @Logger = options[:logger]
        else
          require 'logger'
          @Logger = proc{|path|
            logger = Logger.new(path, 1)
            logger.level = Logger::WARN
            logger
          }
        end
      end
      private :init_options

      attr_reader :read_only
      attr_reader :number_of_read_io
      attr_reader :data_hash_type
      attr_reader :jlog_sync
      attr_reader :jlog_hash_type
      attr_reader :jlog_rotate_size
      attr_reader :jlog_rotate_max
    end
    include InitOptions

    def self.load_conf(path)
      conf = YAML.load(IO.read(path))
      options = {}
      for name, value in conf
        case (name)
        when 'data_hash_type', 'jlog_hash_type'
          value = value.to_sym
        when 'properties_cache_limit_size', 'master_cache_limit_size'
          name = name.sub(/_limit_size$/, '')
          value = LRUCache.new(value)
        when 'logging_level'
          require 'logger'
          name = 'logger'
          level = case (value)
                  when 'debug', 'info', 'warn', 'error', 'fatal'
                    Logger.const_get(value.upcase)
                  else
                    raise "unknown logging level: #{value}"
                  end
          value = proc{|path|
            logger = Logger.new(path, 1)
            logger.level = level
            logger
          }
        end
        options[name.to_sym] = value
      end
      options
    end

    # <tt>name</tt> is storage name.
    # see Higgs::Storage::InitOptions for <tt>options</tt>.
    #
    # storage is composed of the following 5 files.
    # <tt>name.log</tt>:: event log. default logging level is <tt>WARN</tt>.
    # <tt>name.tar</tt>:: data file compatible with unix TAR format.
    # <tt>name.idx</tt>:: index snapshot. genuine index is Hash in memory.
    #                     see Higgs::MVCCIndex for detail.
    # <tt>name.jlog</tt>:: transaction journal log. see Higgs::JournalLogger for detail.
    # <tt>name.lock</tt>:: lock file for File#flock. see Higgs::FileLock for detail.
    #
    def initialize(name, options={})
      @name = name
      @log_name = "#{@name}.log"
      @tar_name = "#{@name}.tar"
      @idx_name = "#{@name}.idx"
      @jlog_name = "#{@name}.jlog"
      @lock_name = "#{@name}.lock"

      @commit_lock = Mutex.new
      @state_lock = Mutex.new
      @panic = false
      @shutdown = false

      init_options(options)

      init_completed = false
      read_only = @read_only && @read_only != :standby
      begin
        @flock = FileLock.new(@lock_name, read_only)
        if (read_only) then
          @flock.read_lock
        else
          @flock.write_lock
        end

        @logger = @Logger.call(@log_name)
        @logger.info("storage open start...")
        if (read_only) then
          @logger.info("get file lock for read")
        else
          @logger.info("get file lock for write")
        end

        @logger.info format('block format version: 0x%04X', Block::FMT_VERSION)
        @logger.info("journal log hash type: #{@jlog_hash_type}")
        @logger.info("index format version: #{MVCCIndex::MAJOR_VERSION}.#{MVCCIndex::MINOR_VERSION}")
        @logger.info("storage data hash type: #{@data_hash_type}")
        @logger.info("storage properties cksum type: #{PROPERTIES_CKSUM_TYPE}")

        @logger.info("properties cache type: #{@properties_cache.class}")
        @p_cache = SharedWorkCache.new(@properties_cache) {|cnum_key_pair|
          cnum, key = cnum_key_pair
          value = read_record_body(cnum, key, :p) and decode_properties(key, value)
        }

        @logger.info("data cache type: #{@data_cache.class}")
        @d_cache = SharedWorkCache.new(@data_cache) {|cnum_key_pair|
          cnum, key = cnum_key_pair
          value = read_data(cnum, key) and value.freeze
        }

        unless (read_only) then
          begin
            w_io = File.open(@tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
            @logger.info("create and open I/O handle for write: #{@tar_name}")
          rescue Errno::EEXIST
            @logger.info("open I/O handle for write: #{@tar_name}")
            w_io = File.open(@tar_name, File::WRONLY, 0660)
          end
          w_io.binmode
          w_io.set_encoding(Encoding::ASCII_8BIT)
          @w_tar = Tar::ArchiveWriter.new(w_io)
        end

        @logger.info("build I/O handle pool for read.")
        @r_tar_pool = Pool.new(@number_of_read_io) {
          r_io = Tar::RawIO.open(@tar_name, File::RDONLY)
          @logger.info("open I/O handle for read: #{@tar_name}")
          Tar::ArchiveReader.new(r_io)
        }

        @index = MVCCIndex.new
        if (File.exist? @idx_name) then
          @logger.info("load index: #{@idx_name}")
          @index.load(@idx_name)
          unless (@index.storage_id) then # for migration to new index format of version 0.2 from old version
            @index.storage_id = create_storage_id
            @logger.info("save storage id: #{@index.storage_id}")
            @index.save(@idx_name)
          end

          # index type migration
          unless (@index.index_type) then
            @index.transaction{|cnum|
              next_cnum = cnum.succ
              @r_tar_pool.transaction{|r_tar|
                @index.each_key(cnum) do |key|
                  i = @index[cnum, key] or raise PanicError, "not found an index entry at: #{key}"
                  i = i.dup
                  i.each do |type, j|
                    r_tar.seek(j[:pos])
                    head = r_tar.read_header or raise PanicError, "unexpected EOA at: #{key}"
                    i[type] = { :pos => j[:pos], :siz => head[:size], :cnum => next_cnum }
                  end
                  @index[next_cnum, key] = i
                end
              }
              @index.succ!
            }
            @index.index_type = '1'
          end
          (@index.index_type == '1') or raise PanicError, "uknown index type: #{@index.index_type}"
        else
          @index.storage_id = create_storage_id
          @index.index_type = '1'
          @logger.info("save storage id: #{@index.storage_id}")
          @index.save(@idx_name)
        end
        if (JournalLogger.need_for_recovery? @jlog_name) then
          recover
        end
        unless (read_only) then
          @logger.info("journal log sync mode: #{@jlog_sync}")
          @logger.info("open journal log for write: #{@jlog_name}")
          @jlog = JournalLogger.open(@jlog_name, @jlog_sync, @jlog_hash_type)
        end

        init_completed = true
      ensure
        if (init_completed) then
          @logger.info("completed storage open.")
        else
          @panic = true

          if ($! && @logger) then
            begin
              @logger.error($!)
            rescue
              # ignore error
            end
          end

          if (@jlog_rotate_service) then
            begin
              @jlog_rotate_service.stop_service
            rescue
              # ignore error
            end
          end

          unless (read_only) then
            if (@jlog) then
              begin
                @jlog.close(false)
              rescue
                # ignore error
              end
            end
          end

          if (@r_tar_pool) then
            @r_tar_pool.shutdown{|r_tar|
              begin
                r_tar.close
              rescue
                # ignore errno
              end
            }
          end

          unless (read_only) then
            if (@w_tar) then
              begin
                @w_tar.close(false)
              rescue
                # ignore error
              end
            end
          end

          if (@logger) then
            begin
              @logger.fatal("abrot storage open.")
              @logger.close
            rescue
              # ignore error
            end
          end

          if (@flock) then
            begin
              @flock.close
            rescue
              # ignore error
            end
          end
        end
      end
    end

    def create_storage_id
      hash = Digest::MD5.new
      now = Time.now
      hash.update(now.to_s)
      hash.update(String(now.usec))
      hash.update(String(rand(0)))
      hash.update(String($$))
      hash.update(@name)
      hash.update('toki')
      hash.hexdigest
    end
    private :create_storage_id

    def check_panic
      if (@shutdown) then
        raise ShutdownException, 'storage shutdown'
      end
      if (@panic) then
        raise PanicError, 'broken storage'
      end
    end
    private :check_panic

    def check_read
      @state_lock.synchronize{
        check_panic
      }
    end
    private :check_read

    def check_standby
      @state_lock.synchronize{
        check_panic
        if (@read_only && @read_only != :standby) then
          raise NotWritableError, 'failed to write to read only storage'
        end
      }
    end
    private :check_standby

    def check_read_write
      @state_lock.synchronize{
        check_panic
        if (@read_only) then
          raise NotWritableError, 'failed to write to read only storage'
        end
      }
    end
    private :check_read_write

    def recover
      @logger.warn('incompleted storage and recover from journal log...')

      check_standby
      recover_completed = false
      begin
        safe_pos = 0
        @logger.info("open journal log for read: #{@jlog_name}")
        File.open(@jlog_name, File::RDONLY) {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          begin
            JournalLogger.scan_log(f) do |log|
              change_number = log[0]
              @logger.info("apply journal log: #{change_number}")
              Storage.apply_journal(@w_tar, @index, log)
            end
          rescue Block::BrokenError
            # nothing to do.
          end
          safe_pos = f.tell
        }
        @logger.info("last safe point of journal log: #{safe_pos}")

        @logger.info("flush storage.")
        @w_tar.flush

        File.open(@jlog_name, File::WRONLY, 0660) {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          @logger.info("shrink journal log to erase last broken segment.")
          f.truncate(safe_pos)
          f.seek(safe_pos)
          @logger.info("write eof mark to journal log.")
          JournalLogger.eof_mark(f)
        }

        recover_completed = true
      ensure
        unless (recover_completed) then
          @state_lock.synchronize{ @panic = true }
          @logger.error("panic: failed to recover.")
          @logger.error($!) if $!
        end
      end

      @logger.info('completed recovery from journal log.')
    end
    private :recover

    attr_reader :name

    def shutdown
      @commit_lock.synchronize{
        @state_lock.synchronize{
          read_only = @read_only && @read_only != :standby

          if (@shutdown) then
            raise ShutdownException, 'storage shutdown'
          end
          @logger.info("shutdown start...")
          @shutdown = true

          if (@jlog_rotate_service) then
            @logger.info("stop journal log rotation service: #{@jlog_rotate_service}")
            @jlog_rotate_service.stop_service
          end

          unless (read_only) then
            if (@panic) then
              @logger.warn("abort journal log: #{@jlog_name}")
              @jlog.close(false)
            else
              @logger.info("close journal log: #{@jlog_name}")
              @jlog.close
            end
          end

          if (! @panic && ! read_only) then
            @logger.info("save index: #{@idx_name}")
            @index.save(@idx_name)
          end

          @r_tar_pool.shutdown{|r_tar|
            @logger.info("close I/O handle for read: #{@tar_name}")
            r_tar.close
          }
          unless (read_only) then
            @logger.info("sync write data: #{@tar_name}")
            @w_tar.fsync
            @logger.info("close I/O handle for write: #{@tar_name}")
            @w_tar.close(false)
          end

          @logger.info("unlock: #{@lock_name}")
          @flock.close

          @logger.info("completed shutdown.")
          @logger.close
        }
      }
      nil
    end

    def shutdown?
      @state_lock.synchronize{ @shutdown }
    end

    def alive?
      @state_lock.synchronize{
        ! @shutdown && ! @panic
      }
    end

    def self.rotated_entries(name)
      rotate_list = Dir["#{name}.*"].map{|nm|
        n = Integer(nm[(name.length + 1)..-1])
        [ nm, n ]
      }.sort{|a, b|
        a[1] <=> b[1]
      }.map{|nm, n|
        nm
      }
      rotate_list
    end

    def internal_rotate_journal_log(save_index)
      @logger.info("start journal log rotation.")

      rot_jlog_name = nil
      @index.transaction{|cnum|
        commit_log = []
        while (File.exist? "#{@jlog_name}.#{@index.change_number}")
          @index.succ!
          @logger.debug("index succ: #{@index.change_number}") if @logger.debug?
          commit_log << { :ope => :succ, :cnum => @index.change_number }
        end
        unless (commit_log.empty?) then
          @logger.debug("write journal log: #{@index.change_number}") if @logger.debug?
          @jlog.write([ @index.change_number, commit_log, @index.storage_id, @index.index_type ])
        end
        rot_jlog_name = "#{@jlog_name}.#{@index.change_number}"

        if (save_index) then
          case (save_index)
          when String
            @logger.info("save index: #{save_index}")
            @index.save(save_index)
          else
            @logger.info("save index: #{@idx_name}")
            @index.save(@idx_name)
          end
        else
          @logger.info("no save index.")
        end
      }

      @logger.info("close journal log.")
      @jlog.close
      @logger.info("rename journal log: #{@jlog_name} -> #{rot_jlog_name}")
      File.rename(@jlog_name, rot_jlog_name)
      if (@jlog_rotate_max > 0) then
        rotate_list = Storage.rotated_entries(@jlog_name)
        while (rotate_list.length > @jlog_rotate_max)
          unlink_jlog_name = rotate_list.shift
          @logger.info("unlink old journal log: #{unlink_jlog_name}")
          File.unlink(unlink_jlog_name)
        end
      end
      @logger.info("open journal log: #{@jlog_name}")
      @jlog = JournalLogger.open(@jlog_name, @jlog_sync, @jlog_hash_type)

      @logger.info("completed journal log rotation.")
    end
    private :internal_rotate_journal_log

    def rotate_journal_log(save_index=true)
      @commit_lock.synchronize{
        check_read_write
        rotate_completed = false
        begin
          internal_rotate_journal_log(save_index)
          rotate_completed = true
        ensure
          unless (rotate_completed) then
            @state_lock.synchronize{ @panic = true }
            @logger.error("panic: failed to rotate journal log.")
            @logger.error($!) if $!
          end
        end
      }

      nil
    end

    class TransactionHandler
      def initialize(storage, change_number)
        @storage = storage
        @cnum = change_number
      end

      # methods to write.

      def raw_write_and_commit(write_list, *optional)
        @storage.raw_write_and_commit(write_list, *optional)
        @cnum = @storage.change_number
        nil
      end

      def write_and_commit(write_list, *optional)
        @storage.write_and_commit(@cnum, write_list, *optional)
        @cnum = @storage.change_number
        nil
      end

      def change_number
        @cnum
      end

      # methods to read.

      def fetch_properties(key)
        @storage.fetch_properties(@cnum, key)
      end

      def fetch_data(key)
        @storage.fetch_data(@cnum, key)
      end

      def data_change_number(key)
        @storage.data_change_number(@cnum, key)
      end

      def properties_change_number(key)
        @storage.properties_change_number(@cnum, key)
      end

      def key?(key)
        @storage.key?(@cnum, key)
      end

      def keys(*optional)
        @storage.keys(@cnum, *optional)
      end

      def each_key(&block)
        @storage.each_key(@cnum, &block)
      end
    end

    class ReadHandler < TransactionHandler
      undef raw_write_and_commit
      undef write_and_commit
    end

    class WriteHandler < TransactionHandler
    end

    # <tt>tx</tt> is storage handler to read or write.
    def transaction(read_only=false) # :yields: tx
      if (@read_only || read_only) then
        @index.transaction{|cnum|
          yield(ReadHandler.new(self, cnum))
        }
      else
        @index.transaction{|cnum|
          yield(WriteHandler.new(self, cnum))
        }
      end
    end

    # internal utility functions
    module IndexUtils
      def tar_blocked_size(siz)
        Tar::Block::BLKSIZ + Tar::Block.blocked_size(siz)
      end
      module_function :tar_blocked_size
    end
    extend IndexUtils
    include IndexUtils

    # should be called in a block of transaction method.
    def raw_write_and_commit(write_list, commit_time=Time.now)
      @commit_lock.synchronize{
        @logger.debug("start raw_write_and_commit.") if @logger.debug?

        cnum = @index.change_number
        check_read_write
        commit_log = []
        commit_completed = false
        eoa = @index.eoa

        begin
          (@index.change_number == cnum) or raise 'internal error.' # assertion
          next_cnum = cnum.succ

          for ope, key, type, name, value in write_list
            case (ope)
            when :write
              @logger.debug("journal log for write: (key,type)=(#{key},#{type})") if @logger.debug?
              unless (value.kind_of? String) then
                raise TypeError, "can't convert #{value.class} (value) to String"
              end
              blocked_size = tar_blocked_size(value.bytesize)

              # recycle
              if (pos = @index.free_fetch(blocked_size)) then
                @logger.debug("write type of recycle free region: (pos,size)=(#{pos},#{blocked_size})") if @logger.debug?
                commit_log << {
                  :ope => :free_fetch,
                  :pos => pos,
                  :siz => value.bytesize
                }
                commit_log << {
                  :ope => :write,
                  :key => key,
                  :pos => pos,
                  :typ => type,
                  :mod => commit_time,
                  :nam => name,
                  :val => value
                }
                if (i = @index[next_cnum, key]) then
                  i = i.dup
                  if (j = i[type]) then
                    j = j.dup; i[type] = j
                    commit_log << {
                      :ope => :free_store,
                      :pos => j[:pos],
                      :siz => j[:siz],
                      :mod => commit_time
                    }
                    @index.free_store(j[:pos], tar_blocked_size(j[:siz]))
                    j[:pos] = pos
                    j[:siz] = value.bytesize
                    j[:cnum] = next_cnum
                  else
                    i[type] = { :pos => pos, :siz => value.bytesize, :cnum => next_cnum }
                  end
                  @index[next_cnum, key] = i
                else
                  @index[next_cnum, key] = { type => { :pos => pos, :siz => value.bytesize, :cnum => next_cnum } }
                end
                next
              end

              # append
              @logger.debug("write type of append: (pos,size)=(#{eoa},#{blocked_size})")
              commit_log << {
                :ope => :write,
                :key => key,
                :pos => eoa,
                :typ => type,
                :mod => commit_time,
                :nam => name,
                :val => value
              }
              if (i = @index[next_cnum, key]) then
                i = i.dup
                if (j = i[type]) then
                  j = j.dup; i[type] = j
                  commit_log << {
                    :ope => :free_store,
                    :pos => j[:pos],
                    :siz => j[:siz],
                    :mod => commit_time
                  }
                  @index.free_store(j[:pos], tar_blocked_size(j[:siz]))
                  j[:pos] = eoa
                  j[:siz] = value.bytesize
                  j[:cnum] = next_cnum
                else
                  i[type] = { :pos => eoa, :siz => value.bytesize, :cnum => next_cnum }
                end
                @index[next_cnum, key] = i
              else
                @index[next_cnum, key] = { type => { :pos => eoa, :siz => value.bytesize, :cnum => next_cnum } }
              end
              eoa += blocked_size
              commit_log << { :ope => :eoa, :pos => eoa }
            when :delete
              @logger.debug("journal log for delete: #{key}") if @logger.debug?
              if (i = @index.delete(next_cnum, key)) then
                commit_log << {
                  :ope => :delete,
                  :key => key
                }
                i.each_value{|j|
                  commit_log << {
                    :ope => :free_store,
                    :pos => j[:pos],
                    :siz => j[:siz],
                    :mod => commit_time
                  }
                  @index.free_store(j[:pos], tar_blocked_size(j[:siz]))
                }
              end
            else
              raise ArgumentError, "unknown operation: #{cmd[:ope]}"
            end
          end

          @logger.debug("index succ: #{next_cnum}") if @logger.debug?
          commit_log << { :ope => :succ, :cnum => next_cnum }

          @logger.debug("write journal log: #{next_cnum}") if @logger.debug?
          @jlog.write([ next_cnum, commit_log, @index.storage_id, @index.index_type ])

          for cmd in commit_log
            case (cmd[:ope])
            when :write
              name = cmd[:nam][0, Tar::Block::MAX_LEN]
              @logger.debug("write data to storage: (name,pos,size)=(#{name},#{cmd[:pos]},#{cmd[:val].size})") if @logger.debug?
              @w_tar.seek(cmd[:pos])
              @w_tar.add(name, cmd[:val], :mtime => cmd[:mod])
            when :free_store
              @logger.debug("write free region to storage: (pos,size)=(#{cmd[:pos]},#{cmd[:siz]})") if @logger.debug?
              name = format('.free.%x', cmd[:pos] >> 9)
              @w_tar.seek(cmd[:pos])
              @w_tar.write_header(:name => name, :size => cmd[:siz], :mtime => cmd[:mod])
            when :delete, :eoa, :free_fetch, :succ
              # nothing to do.
            else
              raise "unknown operation: #{cmd[:ope]}"
            end
          end
          if (@index.eoa != eoa) then
            @logger.debug("write EOA to storage: #{eoa}")
            @index.eoa = eoa
            @w_tar.seek(eoa)
            @w_tar.write_EOA
          end
          @logger.debug("flush storage.")
          @w_tar.flush

          # release updated entries.
          @index.succ!

          if (@jlog_rotate_size > 0 && @jlog.size >= @jlog_rotate_size) then
            internal_rotate_journal_log(true)
          end

          commit_completed = true
        ensure
          unless (commit_completed) then
            @state_lock.synchronize{ @panic = true }
            @logger.error("panic: failed to commit.")
            @logger.error($!) if $!
          end
        end

        @logger.debug("completed raw_write_and_commit.") if @logger.debug?
      }

      nil
    end

    def self.apply_journal(w_tar, index, log)
      change_number, commit_log, storage_id, index_type = log

      if (storage_id) then # check for backward compatibility
        if (storage_id != index.storage_id) then
          raise PanicError, "unexpected storage id: expected <#{index.storage_id}> but was <#{storage_id}>"
        end
      end

      if (change_number - 1 < index.change_number) then
        # skip old jounal log
      elsif (change_number - 1 > index.change_number) then
        raise PanicError, "lost journal log (cnum: #{index.change_number.succ})"
      else # if (change_number - 1 == index.change_number) then
        if (index_type != index.index_type) then
          raise PanicError, "unexpected index type: expected <#{index.index_type}> but was <#{index_type}>"
        end
        index.transaction{|cnum|
          next_cnum = cnum.succ
          for cmd in commit_log
            case (cmd[:ope])
            when :write
              yield(cmd[:key]) if block_given?
              name = "#{cmd[:key]}.#{cmd[:typ]}"[0, Tar::Block::MAX_LEN]
              w_tar.seek(cmd[:pos])
              w_tar.add(cmd[:nam], cmd[:val], :mtime => cmd[:mod])
              if (i = index[next_cnum, cmd[:key]]) then
                i = i.dup
                if (j = i[cmd[:typ]]) then
                  j = j.dup; i[cmd[:typ]] = j
                  j[:pos] = cmd[:pos]
                  j[:siz] = cmd[:val].bytesize
                  j[:cnum] = next_cnum
                else
                  i[cmd[:typ]] = { :pos => cmd[:pos], :siz => cmd[:val].bytesize, :cnum => next_cnum }
                end
                index[next_cnum, cmd[:key]] = i
              else
                index[next_cnum, cmd[:key]] = { cmd[:typ] => { :pos => cmd[:pos], :siz => cmd[:val].bytesize, :cnum => next_cnum } }
              end
            when :delete
              yield(cmd[:key]) if block_given?
              index.delete(next_cnum, cmd[:key])
            when :free_fetch
              index.free_fetch_at(cmd[:pos], tar_blocked_size(cmd[:siz]))
            when :free_store
              index.free_store(cmd[:pos], tar_blocked_size(cmd[:siz]))
              name = format('.free.%x', cmd[:pos] >> 9)
              w_tar.seek(cmd[:pos])
              w_tar.write_header(:name => name, :size => cmd[:siz], :mtime => cmd[:mod])
            when :eoa
              index.eoa = cmd[:pos]
              w_tar.seek(cmd[:pos])
              w_tar.write_EOA
            when :succ
              index.succ!
              if (index.change_number != cmd[:cnum]) then
                raise PanicError, "invalid journal log (succ: #{cmd[:cnum]})"
              end
            else
              raise "unknown operation: #{cmd[:ope]}"
            end
          end
        }
      end

      nil
    end

    def self.recover(name, out=nil, verbose_level=1)
      tar_name = "#{name}.tar"
      idx_name = "#{name}.idx"
      jlog_name = "#{name}.jlog"
      lock_name = "#{name}.lock"

      FileLock.open(lock_name) {|flock|
        flock.synchronize{
          begin
            w_io = File.open(tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
          rescue Errno::EEXIST
            w_io = File.open(tar_name, File::WRONLY, 0660)
          end
          w_io.binmode
          w_io.set_encoding(Encoding::ASCII_8BIT)
          w_tar = Tar::ArchiveWriter.new(w_io)

          index = MVCCIndex.new
          index.load(idx_name) if (File.exist? idx_name)

          out << "recovery target: #{name}\n" if (out && verbose_level >= 1)
          jlog_list = rotated_entries(jlog_name)
          jlog_list << jlog_name if (File.exist? jlog_name)
          for curr_name in jlog_list
            begin
              JournalLogger.each_log(curr_name) do |log|
                change_number = log[0]
                out << "apply journal log: #{change_number}\n" if (out && verbose_level >= 1)
                apply_journal(w_tar, index, log)
              end
            rescue Block::BrokenError
              out << "warning: incompleted journal log and stopped at #{curr_name}\n" if out
            end
          end

          index.save(idx_name)
          w_tar.fsync
          w_tar.close(false)
        }
      }

      nil
    end

    def apply_journal_log(path)
      @commit_lock.synchronize{
        @logger.info("start to apply journal log.")

        check_standby
        apply_completed = false
        begin
          JournalLogger.each_log(path) do |log|
            change_number, commit_log, storage_id = log

            if (storage_id) then # check for backward compatibility
              if (storage_id != @index.storage_id) then
                raise PanicError, "unexpected storage id: expected <#{@index.storage_id}> but was <#{storage_id}>"
              end
            end

            if (change_number - 1 < @index.change_number) then
              @logger.debug("skip journal log: #{change_number}") if @logger.debug?
            elsif (change_number - 1 > @index.change_number) then
              raise PanicError, "lost journal log (cnum: #{@index.change_number + 1})"
            else # if (change_number - 1 == @index.change_number) then
              @logger.debug("write journal log: #{change_number}") if @logger.debug?
              @jlog.write(log)

              @logger.debug("apply journal log: #{change_number}") if @logger.debug?
              Storage.apply_journal(@w_tar, @index, log) {|key|
                yield(key) if block_given?
              }

              if (@jlog_rotate_size > 0 && @jlog.size >= @jlog_rotate_size) then
                internal_rotate_journal_log(true)
              end
            end
          end

          @logger.debug("flush storage.")
          @w_tar.flush

          apply_completed = true
        ensure
          unless (apply_completed) then
            @state_lock.synchronize{ @panic = true }
            @logger.error("panic: failed to apply journal log.")
            @logger.error($!) if $!
          end
        end

        @logger.info("completed to apply journal log.")
      }

      nil
    end

    def switch_to_write
      @state_lock.synchronize{
        if (@read_only != :standby) then
          raise "not standby mode: #{@read_only}"
        end
        @read_only = false
      }
      nil
    end

    # should be called in a block of transaction method.
    def write_and_commit(cnum, write_list, commit_time=Time.now)
      check_read_write

      raw_write_list = []
      deleted_entries = {}
      update_properties = {}

      for ope, key, value in write_list
        case (ope)
        when :write
          unless (value.kind_of? String) then
            raise TypeError, "can't convert #{value.class} (value) to String"
          end
          raw_write_list << [ :write, key, :d, key.to_s, value ]
          deleted_entries[key] = false
          if (properties = update_properties[key]) then
            # nothing to do.
          elsif (properties = internal_fetch_properties(cnum, key)) then
            properties = properties.dup
            properties['system_properties'] = properties['system_properties'].dup
            update_properties[key] = properties
          else
            # new properties
            properties = {
              'system_properties' => {
                'hash_type' => @data_hash_type.to_s,
                'hash_value' => nil,
                'created_time' => commit_time,
                'changed_time' => commit_time,
                'modified_time' => nil,
                'string_only' => false
              },
              'custom_properties' => {}
            }
            update_properties[key] = properties
          end
          properties['system_properties']['hash_value'] = DATA_HASH[@data_hash_type].call(value)
          properties['system_properties']['modified_time'] = commit_time
        when :delete
          raw_write_list << [ :delete, key ]
          deleted_entries[key] = true
          update_properties.delete(key)
        when :custom_properties, :system_properties
          if (deleted_entries[key]) then
            raise IndexError, "not exist properties at key: #{key}"
          end
          if (properties = update_properties[key]) then
            # nothing to do.
          elsif (properties = internal_fetch_properties(cnum, key)) then
            properties = properties.dup
            properties['system_properties'] = properties['system_properties'].dup
            update_properties[key] = properties
          else
            raise IndexError, "not exist properties at key: #{key}"
          end
          properties['system_properties']['changed_time'] = commit_time
          case (ope)
          when :custom_properties
            properties['custom_properties'] = value
          when :system_properties
            if (value.key? 'string_only') then
              properties['system_properties']['string_only'] = value['string_only'] ? true : false
            end
          else
            raise ArgumentError, "unknown operation: #{ope}"
          end
        else
          raise ArgumentError, "unknown operation: #{ope}"
        end
      end

      next_cnum = @index.change_number.succ
      for key, properties in update_properties
        cnum_key_pair = [ next_cnum, key ]
        @p_cache[cnum_key_pair] = properties.higgs_deep_freeze
        raw_write_list << [ :write, key, :p, "#{key}.p", encode_properties(properties) ]
      end

      raw_write_and_commit(raw_write_list, commit_time)

      nil
    end

    # should be called in a block of transaction method.
    def read_record(cnum, key, type)
      head_and_body = nil
      if (i = @index[cnum, key]) then
        if (j = i[type]) then
          @r_tar_pool.transaction{|r_tar|
            r_tar.seek(j[:pos])
            head_and_body = r_tar.fetch
          }
          unless (head_and_body) then
            @state_lock.synchronize{ @panic = true }
            @logger.error("panic: failed to read record: #{key}")
            raise PanicError, "failed to read record: #{key}"
          end
        end
      end
      head_and_body
    end
    private :read_record

    # should be called in a block of transaction method.
    def read_record_body(cnum, key, type)
      head_and_body = read_record(cnum, key, type) or return
      head_and_body[:body]
    end
    private :read_record_body

    def encode_properties(properties)
      body = properties.to_yaml
      head = "\# #{PROPERTIES_CKSUM_TYPE} #{body.sum(PROPERTIES_CKSUM_BITS)}\n"
      head + body
    end
    private :encode_properties

    def decode_properties(key, value)
      head, body = value.split(/\n/, 2)
      cksum_type, cksum_value = head.sub(/^#\s+/, '').split(/\s+/, 2)
      if (cksum_type != PROPERTIES_CKSUM_TYPE) then
        @state_lock.synchronize{ @panic = true }
        @logger.error("panic: unknown properties cksum type: #{cksum_type}")
        raise PanicError, "unknown properties cksum type: #{cksum_type}"
      end
      if (body.sum(PROPERTIES_CKSUM_BITS) != Integer(cksum_value)) then
        @state_lock.synchronize{ @panic = true }
        @logger.error("panic: mismatch properties cksum at #{key}")
        raise PanicError, "mismatch properties cksum at #{key}"
      end
      YAML.load(body).higgs_deep_freeze
    end
    private :decode_properties

    # should be called in a block of transaction method.
    def internal_fetch_properties(cnum, key)
      cnum_key_pair = [ cnum, key ]
      @p_cache[cnum_key_pair]   # see initialize.
    end
    private :internal_fetch_properties

    # should be called in a block of transaction method.
    def read_data(cnum, key)
      value = read_record_body(cnum, key, :d) or return
      unless (properties = internal_fetch_properties(cnum, key)) then
        @state_lock.synchronize{ @panic = true }
        @logger.error("panic: failed to read properties: #{key}")
        raise PanicError, "failed to read properties: #{key}"
      end
      hash_type = properties['system_properties']['hash_type']
      unless (hash_proc = DATA_HASH_BIN[hash_type]) then
        @state_lock.synchronize{ @panic = true }
        @logger.error("panic: unknown data hash type: #{hash_type}")
        raise PanicError, "unknown data hash type: #{hash_type}"
      end
      hash_value = hash_proc.call(value)
      if (hash_value != properties['system_properties']['hash_value']) then
        @state_lock.synchronize{ @panic = true }
        @logger.error("panic: mismatch hash value at #{key}")
        raise PanicError, "mismatch hash value at #{key}"
      end
      value
    end
    private :read_data

    # should be called in a block of transaction method.
    def fetch_properties(cnum, key)
      check_read
      internal_fetch_properties(cnum, key)
    end

    # should be called in a block of transaction method.
    def fetch_data(cnum, key)
      check_read
      cnum_key_pair = [ cnum, key ]
      @d_cache[cnum_key_pair]
    end

    def change_number
      @index.change_number
    end

    # should be called in a block of transaction method.
    def data_change_number(cnum, key)
      i = @index[cnum, key] and i[:d][:cnum] || -1
    end

    # should be called in a block of transaction method.
    def properties_change_number(cnum, key)
      i = @index[cnum, key] and i[:p][:cnum] || -1
    end

    # should be called in a block of transaction method.
    def key?(cnum, key)
      check_read
      @index.key?(cnum, key)
    end

    # should be called in a block of transaction method.
    def keys(cnum, order_by_pos=false)
      keys = @index.keys(cnum)
      if (order_by_pos) then
        keys.sort!{|a, b|
          @index[cnum, a][:d][:pos] <=> @index[cnum, b][:d][:pos]
        }
      end
      keys
    end

    def each_key(cnum)
      check_read
      @index.each_key(cnum) do |key|
        yield(key)
      end
      self
    end

    VERIFY_VERBOSE_LIST = [
      [ 'hash_type', proc{|type| type } ],
      [ 'hash_value', proc{|value| value } ],
      [ 'created_time',
        proc{|t| t.strftime('%Y-%m-%d %H:%M:%S.') + format('%03d', Integer(t.to_f % 1000)) } ],
      [ 'changed_time',
        proc{|t| t.strftime('%Y-%m-%d %H:%M:%S.') + format('%03d', Integer(t.to_f % 1000)) } ],
      [ 'modified_time',
        proc{|t| t.strftime('%Y-%m-%d %H:%M:%S.') + format('%03d', Integer(t.to_f % 1000)) } ],
      [ 'string_only', proc{|flag| flag.to_s } ]
    ]

    # storage access is blocked while this method is processing.
    def verify(out=nil, verbose_level=1)
      @commit_lock.synchronize{
        check_read
        @index.transaction{|cnum|

          key_pos_alist = []
          block_set = {}

          @index.each_block_element do |elem|
            case (elem.block_type)
            when :index
              key_pos_alist << [ elem.key, elem.entry[:d][:pos] ]
              elem.entry.each_value{|j|
                block_set[j[:pos]] = tar_blocked_size(j[:siz])
              }
            when :free, :free_log
              block_set[elem.pos] = elem.size
            else
              raise "unknown block element: #{elem}"
            end
          end

          key_pos_alist.sort_by!{|i| i[1] }
          for key, pos in key_pos_alist
            if (out && verbose_level >= 1) then
              out << "check #{key}\n"
            end
            data = read_data(cnum, key) or raise PanicError, "not exist data at key: #{key}"
            if (out && verbose_level >= 2) then
              out << "  #{data.bytesize} bytes\n"
              properties = fetch_properties(key) or raise PanicError, "not exist properties at key: #{key}"
              for key, format in VERIFY_VERBOSE_LIST
                value = properties['system_properties'][key]
                out << '  ' << key << ': ' << format.call(value) << "\n"
              end
            end
          end

          pos = 0
          @r_tar_pool.transaction{|r_tar|
            while (size = block_set.delete(pos))
              if (out && verbose_level >= 1) then
                out << "check TAR header at #{pos} bytes\n"
              end
              r_tar.seek(pos)
              head = r_tar.read_header
              if (tar_blocked_size(head[:size]) != size) then
                raise PanicError, "broken tar file at #{pos} bytes."
              end
              pos += size
            end
          }

          unless (block_set.empty?) then
            raise PanicError, 'broken tar file.'
          end
          if (@index.eoa != pos) then
            raise PanicError, 'broken index eoa.'
          end
        }
      }

      nil
    end

    class ClientSideLocalhostCheckHandler
      def initialize(path, messg)
        @path = File.expand_path(path)
        @messg = messg
      end

      def call
        unless (File.exist? @path) then
          raise 'client should exist in localhost.'
        end

        File.open(@path, 'r') {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          if (f.read != @messg) then
            raise 'client should exist in localhost.'
          end
        }
        nil
      end
    end

    # check that client exists in localhost.
    def localhost_check
      base_dir = File.dirname(@name)
      tmp_fname = File.join(base_dir, ".localhost_check.#{$$}")
      messg = (0...8).map{ rand(64) }.pack("C*").tr("\x00-\x3f", "A-Za-z0-9./")

      begin
        f = File.open(tmp_fname, File::WRONLY | File::CREAT | File::EXCL, 0644)
      rescue Errno::EEXIST
        tmp_fname.succ!
        retry
      end

      begin
        begin
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          f.write(messg)
        ensure
          f.close
        end
        yield(ClientSideLocalhostCheckHandler.new(tmp_fname, messg))
      ensure
        File.unlink(tmp_fname) if (File.exist? tmp_fname)
      end

      nil
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
