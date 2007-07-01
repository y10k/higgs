# $Id$

require 'higgs/block'
require 'higgs/cache'
require 'higgs/flock'
require 'higgs/index'
require 'higgs/jlog'
require 'higgs/tar'
require 'higgs/thread'
require 'thread'
require 'yaml'

module Higgs
  class Storage
    # for ident(1)
    CVS_ID = '$Id$'

    include Exceptions

    class Error < HiggsError
    end

    class BrokenError < Error
    end

    class NotWritableError < Error
    end

    class ShutdownException < Exceptions::ShutdownException
    end

    PROPERTIES_CKSUM_TYPE = 'SUM16'
    PROPERTIES_CKSUM_BITS = 16

    DATA_HASH = {}
    [ [ :SUM16,  proc{|s| s.sum(16).to_s              },  nil            ],
      [ :MD5,    proc{|s| Digest::MD5.hexdigest(s)    }, 'digest/md5'    ],
      [ :RMD160, proc{|s| Digest::RMD160.hexdigest(s) }, 'digest/rmd160' ],
      [ :SHA1,   proc{|s| Digest::SHA1.hexdigest(s)   }, 'digest/sha1'   ],
      [ :SHA256, proc{|s| Digest::SHA256.hexdigest(s) }, 'digest/sha2'   ],
      [ :SHA384, proc{|s| Digest::SHA384.hexdigest(s) }, 'digest/sha2'   ],
      [ :SHA512, proc{|s| Digest::SHA512.hexdigest(s) }, 'digest/sha2'   ]
    ].each do |hash_symbol, hash_proc, hash_lib|
      if (hash_lib) then
        begin
          require(hash_lib)
        rescue LoadError
          next
        end
      end
      DATA_HASH[hash_symbol] = hash_proc
    end

    DATA_HASH_BIN = {}
    DATA_HASH.each do |cksum_symbol, cksum_proc|
      DATA_HASH_BIN[cksum_symbol.to_s] = cksum_proc
    end

    module InitOptions
      def init_options(options)
        @number_of_read_io = options[:number_of_read_io] || 2

        if (options.include? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        if (options.include? :properties_cache) then
          @properties_cache = options[:properties_cache]
        else
          @properties_cache = LRUCache.new
        end

        @data_hash_type = options[:data_hash_type] || :MD5
        unless (DATA_HASH.key? @data_hash_type) then
          raise "unknown data hash type: #{@data_hash_type}"
        end

        if (options.include? :jlog_sync) then
          @jlog_sync = options[:jlog_sync]
        else
          @jlog_sync = false
        end

        @jlog_hash_type = options[:jlog_hash_type] || :MD5
        @jlog_rotate_size = options[:jlog_rotate_size] || 1024 * 256
        @jlog_rotate_max = options[:jlog_rotate_max] || 1
        @jlog_rotate_service_uri = options[:jlog_rotate_service_uri]

        if (options.include? :logger) then
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
      attr_reader :jlog_rotate_service_uri
    end
    include InitOptions

    def initialize(name, options={})
      @name = name
      @log_name = "#{@name}.log"
      @tar_name = "#{@name}.tar"
      @idx_name = "#{@name}.idx"
      @jlog_name = "#{@name}.jlog"
      @lock_name = "#{@name}.lock"

      @commit_lock = Mutex.new
      @state_lock = Mutex.new
      @broken = false
      @shutdown = false

      init_options(options)

      init_completed = false
      begin
        @flock = FileLock.new(@lock_name, @read_only)
        if (@read_only) then
          @flock.read_lock
        else
          @flock.write_lock
        end

        @logger = @Logger.call(@log_name)
        @logger.info("storage open start...")
        if (@read_only) then
          @logger.info("get file lock for read")
        else
          @logger.info("get file lock for write")
        end

        @logger.info format('block format version: 0x%04X', Block::FMT_VERSION)
        @logger.info("journal log hash type: #{@jlog_hash_type}")
        @logger.info("index format version: #{Index::MAJOR_VERSION}.#{Index::MINOR_VERSION}")
        @logger.info("storage data hash type: #{@data_hash_type}")
        @logger.info("storage properties cksum type: #{PROPERTIES_CKSUM_TYPE}")
        @logger.info("storage properties cksum bits: #{PROPERTIES_CKSUM_BITS} ")

        @logger.info("properties cache type: #{@properties_cache.class}")
        @properties_cache = SharedWorkCache.new(@properties_cache) {|key|
          value = read_record_body(key, :p) and decode_properties(key, value)
        }

        unless (@read_only) then
          begin
            w_io = File.open(@tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
            @logger.info("create and get I/O handle for write: #{@tar_name}")
          rescue Errno::EEXIST
            @logger.info("open I/O handle for write: #{@tar_name}")
            w_io = File.open(@tar_name, File::WRONLY, 0660)
          end
          w_io.binmode
          @w_tar = Tar::ArchiveWriter.new(w_io)
        end

        @logger.info("build I/O handle pool for read.")
        @r_tar_pool = Pool.new(@number_of_read_io) {
          r_io = File.open(@tar_name, File::RDONLY)
          r_io.binmode
          @logger.info("open I/O handle for read: #{@tar_name}")
          Tar::ArchiveReader.new(Tar::RawIO.new(r_io))
        }

        @index = Index.new
        if (File.exist? @idx_name) then
          @logger.info("load index: #{@idx_name}")
          @index.load(@idx_name)
        end
        if (JournalLogger.need_for_recovery? @jlog_name) then
          recover
        end
        unless (@read_only) then
          @logger.info("journal log sync mode: #{@jlog_sync}")
          @logger.info("open journal log for write: #{@jlog_name}")
          @jlog = JournalLogger.open(@jlog_name, @jlog_sync, @jlog_hash_type)
        end

        if (@jlog_rotate_service_uri) then
          @logger.info("start journal log rotation service: #{@jlog_rotate_service_uri}")
          require 'drb'
          @jlog_rotate_service = DRb::DRbServer.new(@jlog_rotate_service_uri,
                                                    method(:rotate_journal_log))
        else
          @jlog_rotate_service = nil
        end

        init_completed = true
      ensure
        if (init_completed) then
          @logger.info("completed storage open.")
        else
          @broken = true

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

          unless (@read_only) then
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

          unless (@read_only) then
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

    def check_consistency
      @state_lock.synchronize{
        if (@shutdown) then
          raise ShutdownException, 'storage shutdown'
        end
        if (@broken) then
          raise BrokenError, 'broken storage'
        end
      }
    end
    private :check_consistency

    def recover
      @logger.warn('incompleted storage and recover from journal log...')

      check_consistency
      if (@read_only) then
        @logger.warn('read only storage is not recoverable.')
        raise NotWritableError, 'need for recovery'
      end

      recover_completed = false
      begin
        safe_pos = 0
        @logger.info("open journal log for read: #{@jlog_name}")
        File.open(@jlog_name, 'r') {|f|
          f.binmode
          begin
            JournalLogger.scan_log(f) {|log|
              change_number = log[0]
              @logger.info("apply journal log: #{change_number}")
              Storage.apply_journal(@w_tar, @index, log)
            }
          rescue Block::BrokenError
            # nothing to do.
          end
          safe_pos = f.tell
        }
        @logger.info("last safe point of journal log: #{safe_pos}")

        File.open(@jlog_name, 'w') {|f|
          @logger.info("shrink journal log to erase last broken segment.")
          f.truncate(safe_pos)
          f.seek(safe_pos)
          @logger.info("write eof mark to journal log.")
          JournalLogger.eof_mark(f)
        }
        recover_completed = true
      ensure
        unless (recover_completed) then
          @state_lock.synchronize{ @broken = true }
          @logger.error("BROKEN: failed to recover.")
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
          if (@shutdown) then
            raise ShutdownException, 'storage shutdown'
          end
          @logger.info("shutdown start...")
          @shutdown = true

          if (@jlog_rotate_service) then
            @logger.info("stop journal log rotation service: #{@jlog_rotate_service}")
            @jlog_rotate_service.stop_service
          end

          unless (@read_only) then
            if (@broken) then
              @logger.warn("abort journal log: #{@jlog_name}")
              @jlog.close(false)
            else
              @logger.info("close journal log: #{@jlog_name}")
              @jlog.close
            end
          end

          if (! @broken && ! @read_only) then
            @logger.info("save index: #{@idx_name}")
            @index.save(@idx_name)
          end

          @r_tar_pool.shutdown{|r_tar|
            @logger.info("close I/O handle for read: #{@tar_name}")
            r_tar.close
          }
          unless (@read_only) then
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

    def self.rotate_entries(name)
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
      @logger.info("start journal log rotation...")

      commit_log = []
      while (File.exist? "#{@jlog_name}.#{@index.change_number}")
        @index.succ!
        @logger.debug("index succ: #{@index.change_number}") if @logger.debug?
        commit_log << { :ope => :succ, :cnum => @index.change_number }
      end
      unless (commit_log.empty?) then
        @logger.debug("write journal log: #{@index.change_number}") if @logger.debug?
        @jlog.write([ @index.change_number, commit_log ])
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

      @logger.info("close journal log.")
      @jlog.close
      @logger.info("rename journal log: #{@jlog_name} -> #{rot_jlog_name}")
      File.rename(@jlog_name, rot_jlog_name)
      if (@jlog_rotate_max > 0) then
        rotate_list = Storage.rotate_entries(@jlog_name)
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
        check_consistency
        if (@read_only) then
          raise NotWritableError, 'failed to write to read only storage'
        end

        rotate_completed = false
        begin
          internal_rotate_journal_log(save_index)
          rotate_completed = true
        ensure
          unless (rotate_completed) then
            @state_lock.synchronize{ @broken = true }
            @logger.error("BROKEN: failed to rotate journal log.")
            @logger.error($!) if $!
          end
        end
      }

      nil
    end

    def raw_write_and_commit(write_list, commit_time=Time.now)
      @commit_lock.synchronize{
        @logger.debug("start raw_write_and_commit.") if @logger.debug?

        check_consistency
        if (@read_only) then
          raise NotWritableError, 'failed to write to read only storage'
        end

        commit_log = []
        commit_completed = false
        eoa = @index.eoa

        begin
          for ope, key, type, value in write_list
            case (ope)
            when :write
              @logger.debug("journal log for write: (key,type)=(#{key},#{type})") if @logger.debug?
              unless (value.kind_of? String) then
                raise TypeError, "can't convert #{value.class} (value) to String"
              end
              blocked_size = Tar::Block::BLKSIZ + Tar::Block.blocked_size(value.length)

              # recycle
              if (pos = @index.free_fetch(blocked_size)) then
                @logger.debug("write type of recycle free segment: (pos,size)=(#{pos},#{blocked_size})") if @logger.debug?
                commit_log << {
                  :ope => :free_fetch,
                  :pos => pos,
                  :siz => blocked_size
                }
                commit_log << {
                  :ope => :write,
                  :key => key,
                  :pos => pos,
                  :typ => type,
                  :mod => commit_time,
                  :val => value
                }
                if (i = @index[key]) then
                  if (j = i[type]) then
                    commit_log << {
                      :ope => :free_store,
                      :pos => j[:pos],
                      :siz => j[:siz],
                      :mod => commit_time
                    }
                    @index.free_store(j[:pos], j[:siz])
                    j[:pos] = pos
                    j[:siz] = blocked_size
                  else
                    i[type] = { :pos => pos, :siz => blocked_size }
                  end
                else
                  @index[key] = { type => { :pos => pos, :siz => blocked_size } }
                end
                next
              end

              # overwrite
              if (i = @index[key]) then
                if (j = i[type]) then
                  if (j[:siz] >= blocked_size) then
                    @logger.debug("write type of overwrite: (pos,size)=(#{j[:pos]},#{blocked_size})") if @logger.debug?
                    commit_log << {
                      :ope => :write,
                      :key => key,
                      :pos => j[:pos],
                      :typ => type,
                      :mod => commit_time,
                      :val => value
                    }
                    if (j[:siz] > blocked_size) then
                      commit_log << {
                        :ope => :free_store,
                        :pos => j[:pos] + blocked_size,
                        :siz => j[:siz] - blocked_size,
                        :mod => commit_time
                      }
                      @logger.debug("tail free segment: (pos,size)=(#{commit_log[-1][:pos]},#{commit_log[-1][:siz]})") if @logger.debug?
                      @index.free_store(commit_log.last[:pos], commit_log.last[:siz])
                      j[:siz] = blocked_size
                    end
                    next
                  end
                end
              end

              # append
              @logger.debug("write type of append: (pos,size)=(#{eoa},#{blocked_size})")
              commit_log << {
                :ope => :write,
                :key => key,
                :pos => eoa,
                :typ => type,
                :mod => commit_time,
                :val => value
              }
              if (i = @index[key]) then
                if (j = i[type]) then
                  commit_log << {
                    :ope => :free_store,
                    :pos => j[:pos],
                    :siz => j[:siz],
                    :mod => commit_time
                  }
                  @index.free_store(j[:pos], j[:siz])
                  j[:pos] = eoa
                  j[:siz] = blocked_size
                else
                  i[type] = { :pos => eoa, :siz => blocked_size }
                end
              else
                @index[key] = { type => { :pos => eoa, :siz => blocked_size } }
              end
              eoa += blocked_size
              commit_log << {
                :ope => :eoa,
                :pos => eoa
              }
            when :delete
              @logger.debug("journal log for delete: #{key}") if @logger.debug?
              if (i = @index.delete(key)) then
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
                  @index.free_store(j[:pos], j[:siz])
                }
              end
            else
              raise ArgumentError, "unknown operation: #{cmd[:ope]}"
            end
          end

          @index.succ!
          @logger.debug("index succ: #{@index.change_number}") if @logger.debug?
          commit_log << { :ope => :succ, :cnum => @index.change_number }

          @logger.debug("write journal log: #{@index.change_number}") if @logger.debug?
          @jlog.write([ @index.change_number, commit_log ])

          for cmd in commit_log
            case (cmd[:ope])
            when :write
              name = "#{cmd[:key]}.#{cmd[:typ]}"[0, Tar::Block::MAX_LEN]
              @logger.debug("write data to storage: (name,pos,size)=(#{name},#{cmd[:pos]},#{cmd[:val].size})") if @logger.debug?
              @w_tar.seek(cmd[:pos])
              @w_tar.add(name, cmd[:val], :mtime => cmd[:mod])
            when :free_store
              @logger.debug("write free segment to storage: (pos,size)=(#{cmd[:pos]},#{cmd[:siz]})") if @logger.debug?
              name = format('.free.%x', cmd[:pos] >> 9)
              @w_tar.seek(cmd[:pos])
              @w_tar.write_header(:name => name, :size => cmd[:siz] - Tar::Block::BLKSIZ, :mtime => cmd[:mod])
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

          if (@jlog_rotate_size > 0 && @jlog.size >= @jlog_rotate_size) then
            internal_rotate_journal_log(true)
          end

          commit_completed = true
        ensure
          unless (commit_completed) then
            @state_lock.synchronize{ @broken = true }
            @logger.error("BROKEN: failed to commit.")
            @logger.error($!) if $!
          end
        end

        @logger.debug("completed raw_write_and_commit.") if @logger.debug?
      }

      nil
    end

    def self.apply_journal(w_tar, index, log)
      change_number, commit_log = log
      if (index.change_number < change_number) then
        for cmd in commit_log
          case (cmd[:ope])
          when :write
            name = "#{cmd[:key]}.#{cmd[:typ]}"[0, Tar::Block::MAX_LEN]
            w_tar.seek(cmd[:pos])
            w_tar.add(name, cmd[:val], :mtime => cmd[:mod])
            blocked_size = Tar::Block::BLKSIZ + Tar::Block.blocked_size(cmd[:val].length)
            if (i = index[cmd[:key]]) then
              if (j = i[cmd[:typ]]) then
                j[:pos] = cmd[:pos]
                j[:siz] = blocked_size
              else
                i[cmd[:typ]] = { :pos => cmd[:pos], :siz => blocked_size }
              end
            else
              index[cmd[:key]] = { cmd[:typ] => { :pos => cmd[:pos], :siz => blocked_size } }
            end
          when :delete
            index.delete(cmd[:key])
          when :free_fetch
            index.free_fetch_at(cmd[:pos], cmd[:siz])
          when :free_store
            index.free_store(cmd[:pos], cmd[:siz])
            name = format('.free.%x', cmd[:pos] >> 9)
            w_tar.seek(cmd[:pos])
            w_tar.write_header(:name => name, :size => cmd[:siz] - Tar::Block::BLKSIZ, :mtime => cmd[:mod])
          when :eoa
            index.eoa = cmd[:pos]
          when :succ
            index.succ!
            if (index.change_number != cmd[:cnum]) then
              raise BrokenError, 'lost journal log'
            end
          else
            raise "unknown operation from #{curr_jlog_name}: #{cmd[:ope]}"
          end
        end
      end
      nil
    end

    def self.recover(name)
      tar_name = "#{name}.tar"
      idx_name = "#{name}.idx"
      jlog_name = "#{name}.jlog"
      lock_name = "#{name}.lock"

      flock = FileLock.new(lock_name)
      flock.synchronize{
        begin
          w_io = File.open(tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
        rescue Errno::EEXIST
          w_io = File.open(tar_name, File::WRONLY, 0660)
        end
        w_io.binmode
        w_tar = Tar::ArchiveWriter.new(w_io)

        index = Index.new
        index.load(idx_name) if (File.exist? idx_name)

        for curr_name in rotate_entries(jlog_name)
          JournalLogger.each_log(curr_name) do |log|
            apply_journal(w_tar, index, log)
          end
        end
        w_tar.seek(index.eoa)
        w_tar.write_EOA

        index.save(idx_name)
        w_tar.fsync
        w_tar.close(false)
      }
      flock.close

      nil
    end

    def write_and_commit(write_list, commit_time=Time.now)
      check_consistency
      if (@read_only) then
        raise NotWritableError, 'failed to write to read only storage'
      end

      raw_write_list = []
      deleted_entries = {}
      update_properties = {}

      for ope, key, value in write_list
        case (ope)
        when :write
          raw_write_list << [ :write, key, :d, value ]
          deleted_entries[key] = false
          if (properties = update_properties[key]) then
            # nothing to do.
          elsif (properties = internal_fetch_properties(key)) then
            update_properties[key] = properties
          else
            # new properties
            properties = {
              'system_properties' => {
                'hash_type' => @data_hash_type.to_s,
                'hash_value' => nil,
                'created_time' => commit_time,
                'changed_time' => commit_time,
                'modified_time' => nil
              },
              'custom_properties' => {}
            }
            update_properties[key] = properties
          end
          properties['system_properties']['hash_value'] = DATA_HASH[@data_hash_type].call(value)
          properties['system_properties']['modified_time'] = commit_time
          @properties_cache.delete(key)
        when :delete
          raw_write_list << [ :delete, key ]
          deleted_entries[key] = true
          update_properties.delete(key)
          @properties_cache.delete(key)
        when :update_properties
          if (deleted_entries[key]) then
            raise IndexError, "not exist properties at key: #{key}"
          end
          if (properties = update_properties[key]) then
            # nothing to do.
          elsif (properties = internal_fetch_properties(key)) then
            update_properties[key] = properties
          else
            raise IndexError, "not exist properties at key: #{key}"
          end
          properties['system_properties']['changed_time'] = commit_time
          properties['custom_properties'] = value
          @properties_cache.delete(key)
        else
          raise ArgumentError, "unknown operation: #{ope}"
        end
      end

      for key, properties in update_properties
        raw_write_list << [ :write, key, :p, encode_properties(properties) ]
      end

      raw_write_and_commit(raw_write_list, commit_time)

      nil
    end

    def read_record(key, type)
      head_and_body = nil
      if (i = @index[key]) then
        if (j = i[type]) then
          @r_tar_pool.transaction{|r_tar|
            r_tar.seek(j[:pos])
            head_and_body = r_tar.fetch
          }
          unless (head_and_body) then
            @state_lock.synchronize{ @broken = true }
            @logger.error("BROKEN: failed to read record: #{key}")
            raise BrokenError, "failed to read record: #{key}"
          end
        end
      end
      head_and_body
    end
    private :read_record

    def read_record_body(key, type)
      head_and_body = read_record(key, type) or return
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
        @state_lock.synchronize{ @broken = true }
        @logger.error("BROKEN: unknown properties cksum type: #{cksum_type}")
        raise BrokenError, "unknown properties cksum type: #{cksum_type}"
      end
      if (body.sum(PROPERTIES_CKSUM_BITS) != Integer(cksum_value)) then
        @state_lock.synchronize{ @broken = true }
        @logger.error("BROKEN: mismatch properties cksum at #{key}")
        raise BrokenError, "mismatch properties cksum at #{key}"
      end
      YAML.load(body)
    end
    private :decode_properties

    def internal_fetch_properties(key)
      @properties_cache[key] # see initialize
    end
    private :internal_fetch_properties

    def fetch_properties(key)
      check_consistency
      internal_fetch_properties(key)
    end

    def fetch(key)
      check_consistency
      value = read_record_body(key, :d) or return
      unless (properties = internal_fetch_properties(key)) then
        @state_lock.synchronize{ @broken = true }
        @logger.error("BROKEN: failed to read properties: #{key}")
        raise BrokenError, "failed to read properties: #{key}"
      end
      hash_type = properties['system_properties']['hash_type']
      unless (cksum_proc = DATA_HASH_BIN[hash_type]) then
        @state_lock.synchronize{ @broken = true }
        @logger.error("BROKEN: unknown data hash type: #{hash_type}")
        raise BrokenError, "unknown data hash type: #{hash_type}"
      end
      hash_value = cksum_proc.call(value)
      if (hash_value != properties['system_properties']['hash_value']) then
        @state_lock.synchronize{ @broken = true }
        @logger.error("BROKEN: mismatch hash value at #{key}")
        raise BrokenError, "mismatch hash value at #{key}"
      end
      value
    end

    def key?(key)
      check_consistency
      @index.key? key
    end

    def each_key
      check_consistency
      @index.each_key do |key|
        yield(key)
      end
      self
    end

    def verify
      check_consistency
      @index.each_key do |key|
        fetch(key)
      end
      nil
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
