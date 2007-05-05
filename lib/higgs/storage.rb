# $Id$

require 'digest/sha2'
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

    DATA_CKSUM_TYPE = 'SHA512'
    PROPERTIES_CKSUM_TYPE = 'sum'
    PROPERTIES_CKSUM_BITS = 16

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

	if (options.include? :jlog_sync) then
	  @jlog_sync = false
	else
	  @jlog_sync = options[:jlog_sync]
	end

	@jlog_rotate_size = options[:jlog_rotate_size] || 1024 * 256
	@jlog_rotate_max = options[:jlog_rotate_max] || 1
      end
      private :init_options

      attr_reader :read_only
      attr_reader :number_of_read_io
      attr_reader :jlog_sync
      attr_reader :jlog_rotate_size
      attr_reader :jlog_rotate_max
    end
    include InitOptions

    def initialize(name, options={})
      @name = name
      @tar_name = "#{@name}.tar"
      @idx_name = "#{@name}.idx"
      @jlog_name = "#{@name}.jlog"
      @lock_name = "#{@name}.lock"

      @commit_lock = Mutex.new
      @state_lock = Mutex.new
      @broken = false
      @shutdown = false

      init_options(options)

      @properties_cache = SharedWorkCache.new(@properties_cache) {|key|
	value = read_record_body(key, :p) and decode_properties(key, value)
      }

      @flock = FileLock.new(@lock_name, @read_only)
      if (@read_only) then
        @flock.read_lock
      else
        @flock.write_lock
      end

      unless (@read_only) then
	begin
	  w_io = File.open(@tar_name, File::WRONLY | File::CREAT | File::EXCL, 0660)
	rescue Errno::EEXIST
	  w_io = File.open(@tar_name, File::WRONLY, 0660)
	end
	w_io.binmode
	@w_tar = Tar::ArchiveWriter.new(w_io)
      end

      @r_tar_pool = Pool.new(@number_of_read_io) {
        r_io = File.open(@tar_name, File::RDONLY)
        r_io.binmode
        Tar::ArchiveReader.new(Tar::RawIO.new(r_io))
      }

      @index = Index.new
      @index.load(@idx_name) if (File.exist? @idx_name)
      recover if (JournalLogger.need_for_recovery? @jlog_name)
      @jlog = JournalLogger.open(@jlog_name, @jlog_sync) unless @read_only
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
      check_consistency
      if (@read_only) then
	raise NotWritableError, 'need for recovery'
      end

      safe_pos = 0
      File.open(@jlog_name, 'r') {|f|
	f.binmode
	begin
	  JournalLogger.scan_log(f) {|log|
	    Storage.apply_journal(@w_tar, @index, log)
	  }
	rescue Block::BrokenError
	  # nothing to do.
	end
	safe_pos = f.tell
      }

      File.open(@jlog_name, 'w') {|f|
	f.truncate(safe_pos)
	f.seek(safe_pos)
	JournalLogger.eof_mark(f)
      }
    end
    private :recover

    attr_reader :name

    def shutdown
      @commit_lock.synchronize{
        @state_lock.synchronize{
	  if (@shutdown) then
	    raise ShutdownException, 'storage shutdown'
	  end
	  @shutdown = true

          @jlog.close unless @read_only
          @index.save(@idx_name) if (! @broken && ! @read_only)
          @r_tar_pool.shutdown{|r_tar| r_tar.close }
          unless (@read_only) then
            @w_tar.fsync
            @w_tar.close(true)
          end
          @flock.close
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

      if (File.exist? name) then
	rotate_list << name
      end

      rotate_list
    end

    def internal_rotate_journal_log(sync_index)
      commit_log = []
      while (File.exist? "#{@jlog_name}.#{@index.change_number}")
        @index.succ!
	commit_log << { :ope => :succ }
      end
      unless (commit_log.empty?) then
	@jlog.write([ @index.change_number, commit_log ])
      end
      rot_jlog_name = "#{@jlog_name}.#{@index.change_number}"

      if (sync_index) then
        case (sync_index)
        when String
          @index.save(sync_index)
        else
          @index.save(@idx_name)
        end
      end

      @jlog.close
      File.rename(@jlog_name, rot_jlog_name)
      if (@jlog_rotate_max > 0) then
	rotate_list = Storage.rotate_entries(@jlog_name)
	while (rotate_list.length > @jlog_rotate_max)
	  File.unlink(rotate_list.shift)
	end
      end
      @jlog = JournalLogger.open(@jlog_name, @jlog_sync)
    end
    private :internal_rotate_journal_log

    def rotate_journal_log(sync_index=false)
      @commit_lock.synchronize{
        check_consistency
        if (@read_only) then
          raise NotWritableError, 'failed to write to read only storage'
        end
	@broken = true
	internal_rotate_journal_log(sync_index)
	@broken = false
      }

      nil
    end

    def raw_write_and_commit(write_list, commit_time=Time.now)
      @commit_lock.synchronize{
        check_consistency
        if (@read_only) then
          raise NotWritableError, 'failed to write to read only storage'
        end
        @broken = true

        commit_log = []
        eoa = @index.eoa

        for ope, key, type, value in write_list
          case (ope)
          when :write
            unless (value.kind_of? String) then
              raise TypeError, "can't convert #{value.class} (value) to String"
            end
            blocked_size = Tar::Block::BLKSIZ + Tar::Block.blocked_size(value.length)

            # recycle
            if (pos = @index.free_fetch(blocked_size)) then
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
            end

            # overwrite
            if (i = @index[key]) then
              if (j = i[type]) then
                if (j[:siz] >= blocked_size) then
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
                    @index.free_store(commit_log.last[:pos], commit_log.last[:siz])
                    j[:siz] = blocked_size
                  end
                  next
                end
              end
            end

            # append
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
	commit_log << { :ope => :succ }

        @jlog.write([ @index.change_number, commit_log ])

        for cmd in commit_log
          case (cmd[:ope])
          when :write
            name = "#{cmd[:key]}.#{cmd[:typ]}"[0, Tar::Block::MAX_LEN]
            @w_tar.seek(cmd[:pos])
            @w_tar.add(name, cmd[:val], :mtime => cmd[:mod])
          when :free_store
            @w_tar.seek(cmd[:pos])
            @w_tar.write_header(:name => '.free', :size => cmd[:siz] - Tar::Block::BLKSIZ, :mtime => cmd[:mod])
          when :delete, :eoa, :free_fetch, :succ
            # nothing to do.
	  else
	    raise "unknown operation: #{cmd[:ope]}"
          end
        end
        if (@index.eoa != eoa) then
          @index.eoa = eoa
          @w_tar.seek(eoa)
          @w_tar.write_EOA
        end
        @w_tar.flush

	if (@jlog_rotate_size > 0 && @jlog.size >= @jlog_rotate_size) then
	  internal_rotate_journal_log(true)
	end

        @broken = false
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
	    w_tar.seek(cmd[:pos])
	    w_tar.write_header(:name => '.free', :size => cmd[:siz] - Tar::Block::BLKSIZ, :mtime => cmd[:mod])
	  when :eoa
	    index.eoa = cmd[:pos]
	  when :succ
	    index.succ!
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
        w_tar.close(true)
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
		'hash_type' => DATA_CKSUM_TYPE,
		'hash_value' => nil,
		'created_time' => commit_time,
		'changed_time' => commit_time,
		'modified_time' => nil
	      },
	      'custom_properties' => {}
	    }
	    update_properties[key] = properties
	  end
	  properties['system_properties']['hash_value'] = Digest::SHA512.hexdigest(value)
	  properties['system_properties']['modified_time'] = commit_time
	  @properties_cache.delete(key)
	when :delete
	  raw_write_list << [ :delete, key ]
	  deleted_entries[key] = true
	  update_properties.delete(key)
	  @properties_cache.delete(key)
	when :update_properties
	  if (deleted_entries[key]) then
	    raise IndexError, "not exist properties at key: #{key.inspect}"
	  end
	  if (properties = update_properties[key]) then
	    # nothing to do.
	  elsif (properties = internal_fetch_properties(key)) then
	    update_properties[key] = properties
	  else
	    raise IndexError, "not exist properties at key: #{key.inspect}"
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
	    raise BrokenError, "failed to read record: #{key.inspect}"
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
      head = "# sum #{body.sum(PROPERTIES_CKSUM_BITS)}\n"
      head + body
    end
    private :encode_properties

    def decode_properties(key, value)
      head, body = value.split(/\n/, 2)
      cksum_type, cksum_value = head.sub(/^#\s+/, '').split(/\s+/, 2)
      if (cksum_type != PROPERTIES_CKSUM_TYPE) then
        @state_lock.synchronize{ @broken = true }
	raise BrokenError, "unknown properties cksum type: #{cksum_type}"
      end
      if (body.sum(PROPERTIES_CKSUM_BITS) != Integer(cksum_value)) then
        @state_lock.synchronize{ @broken = true }
        raise BrokenError, "mismatch properties cksum at #{key.inspect}"
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
        raise BrokenError, "failed to read properties: #{key.inspect}"
      end
      if (properties['system_properties']['hash_type'] != DATA_CKSUM_TYPE) then
        @state_lock.synchronize{ @broken = true }
	raise BrokenError, "unknown data cksum type: #{properties['system_properties']['hash_type']}"
      end
      hash_value = Digest::SHA512.hexdigest(value)
      if (hash_value != properties['system_properties']['hash_value']) then
        @state_lock.synchronize{ @broken = true }
	raise BrokenError, "mismatch hash value at #{key.inspect}"
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
